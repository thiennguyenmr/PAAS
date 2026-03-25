"""
train_qlora_v1.py — QLoRA training + MLflow integration.

QLoRA training + MLflow integration.

Adapter (~26 MB) is uploaded to MLflow instead of the merged full model (6 GB).
When loading for eval/deploy, base model + adapter are loaded on-the-fly via PeftModel.

Usage:
    python -m training.train_qlora_v1 --dataset-version v1
    python -m training.train_qlora_v1 --dataset-version v2 --lora-r 32 --lr 1e-4
    python -m training.train_qlora_v1 --dataset-version v2 --resume-from-version 2
"""

import os
import argparse
from pathlib import Path

import torch
from peft import LoraConfig, get_peft_model, prepare_model_for_kbit_training, PeftModel
from transformers import AutoModelForCausalLM, AutoTokenizer, BitsAndBytesConfig, TrainingArguments
from trl import SFTTrainer
from datasets import Dataset

from evaluation.evaluate_model_v1 import run_evaluation
import mlflow_client



# ── Main training + MLflow pipeline ──────────────────────────────────────────

def run_training(
    model_id: str,
    dataset: Dataset,
    output_dir: str,
    dataset_version: str = "v1",
    lora_r: int = 16,
    lora_alpha: int = 32,
    learning_rate: float = 2e-4,
    max_steps: int = 10,
    registered_model_name: str = None,
    resume_adapter_version: str = None,
) -> dict:
    """
    Full pipeline for one experiment run.

    Args:
        resume_adapter_version: Registry version to continue training from (e.g. "2").
                                 None = fresh training from base model.
    Returns:
        {
            "run_id": str,
            "artifact_uri": str,   # MLflow artifact URI of the LoRA adapter
            "train_loss": float,
        }
    """
    from datetime import date as _date
    _today = _date.today().strftime("%Y%m%d")

    # ── Auto-generate run_name + tags based on training type ──────────────
    if resume_adapter_version:
        if not registered_model_name:
            raise ValueError("registered_model_name is required when using resume_adapter_version")
        run_name = f"qlora-cont-v{resume_adapter_version}-{_today}"
        run_tags = {
            "model_id":               model_id,
            "training_type":          "continued",
            "parent_adapter_version": str(resume_adapter_version),
            "dataset_version":        dataset_version,
        }
        registry_tags = {
            "training_type":   "continued",
            "parent_version":  str(resume_adapter_version),
            "dataset_version": dataset_version,
        }
    else:
        run_name = f"qlora-fresh-{dataset_version}-{_today}"
        run_tags = {
            "model_id":        model_id,
            "training_type":   "fresh",
            "dataset_version": dataset_version,
        }
        registry_tags = {
            "training_type":   "fresh",
            "dataset_version": dataset_version,
        }

    adapter_dir = os.path.join(output_dir, "adapter")

    with mlflow_client.start_run(
        run_name=run_name,
        tags=run_tags,
    ) as run:
        run_id = run.info.run_id

        # ── 1. Log hyperparams ────────────────────────────────────────────
        mlflow_client.log_params({
            "model_id":        model_id,
            "dataset_version": dataset_version,
            "dataset_size":    len(dataset),
            "lora_r":          lora_r,
            "lora_alpha":      lora_alpha,
            "learning_rate":   learning_rate,
            "max_steps":       max_steps,
        })

        # ── 2. Train ──────────────────────────────────────────────────────
        bnb_config = BitsAndBytesConfig(
            load_in_4bit=True,
            bnb_4bit_use_double_quant=True,
            bnb_4bit_quant_type="nf4",
            bnb_4bit_compute_dtype=torch.bfloat16,
        )
        tokenizer = AutoTokenizer.from_pretrained(model_id, trust_remote_code=True)
        tokenizer.pad_token = tokenizer.eos_token
        model = AutoModelForCausalLM.from_pretrained(
            model_id,
            quantization_config=bnb_config,
            device_map="auto",
            trust_remote_code=True,
        )
        model.config.use_cache = False
        model = prepare_model_for_kbit_training(model)

        # ── Fresh: create new LoRA adapter; Continued: load existing from Registry ─
        if resume_adapter_version:
            from mlflow.tracking import MlflowClient as _MlflowClient
            _client = _MlflowClient()
            _mv = _client.get_model_version(registered_model_name, str(resume_adapter_version))
            _tmp_adapter_dir = os.path.join(output_dir, "resumed_adapter")
            print(f"[Training] Loading adapter v{resume_adapter_version} from {_mv.source}")
            mlflow_client.download_model(_mv.source, _tmp_adapter_dir)
            model = PeftModel.from_pretrained(model, _tmp_adapter_dir, is_trainable=True)
        else:
            peft_config = LoraConfig(
                r=lora_r, lora_alpha=lora_alpha, lora_dropout=0.05,
                bias="none", task_type="CAUSAL_LM",
            )
            model = get_peft_model(model, peft_config)

        training_args = TrainingArguments(
            output_dir=adapter_dir,
            max_steps=max_steps,
            per_device_train_batch_size=1,
            gradient_accumulation_steps=1,
            warmup_steps=1,
            learning_rate=learning_rate,
            bf16=True,   # bfloat16 — matches bnb_4bit_compute_dtype, no grad scaler needed
            logging_steps=1,
            save_steps=999,
            report_to="none",
        )
        trainer = SFTTrainer(
            model=model,
            train_dataset=dataset,
            formatting_func=lambda x: x["text"],
            processing_class=tokenizer,
            args=training_args,
        )
        trainer.train()

        # ── 3. Read final train loss directly from trainer state in memory ─
        history = trainer.state.log_history
        losses = [e["loss"] for e in history if "loss" in e]
        loss = losses[-1] if losses else 0.0
        mlflow_client.log_metrics({"train_loss": loss})

        trainer.save_model(adapter_dir)
        tokenizer.save_pretrained(adapter_dir)

        # ── 4. Upload LoRA adapter to MLflow/MinIO (~26 MB, not 6 GB) ────
        artifact_uri, registry_version = mlflow_client.upload_model(
            model_local_path=adapter_dir,
            artifact_path="adapter",
            registered_model_name=registered_model_name,
            registry_tags=registry_tags,
        )

    print(f"\n[Done] run_id={run_id}  loss={loss:.4f}  artifact={artifact_uri}")
    return {
        "run_id":           run_id,
        "artifact_uri":     artifact_uri,
        "train_loss":       loss,
        "registry_version": registry_version,
    }


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--model-id",      default="scb10x/llama3.2-typhoon2-3b-instruct")
    p.add_argument("--dataset-version", default="v1", choices=["v1", "v2"])
    p.add_argument("--output-dir",    default="./outputs/run")
    p.add_argument("--registered-model-name", default=None)
    p.add_argument("--resume-from-version", default=None,
                   help="Registry version to continue training from, e.g. '2'. Omit for fresh training.")
    p.add_argument("--lora-r",        type=int,   default=16)
    p.add_argument("--lora-alpha",    type=int,   default=32)
    p.add_argument("--lr",            type=float, default=2e-4)
    p.add_argument("--max-steps",     type=int,   default=10)
    p.add_argument("--skip-eval",     action="store_true",
                   help="Skip evaluation after training")
    return p.parse_args()


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env", override=False)

    args = parse_args()

    # Connect MLflow
    cfg = mlflow_client.load_config_from_env()
    mlflow_client.connect(cfg)
    
    from dummy_data import get_train_dataset, get_eval_data
    dataset = get_train_dataset(args.dataset_version)
    print(f"[Data] version={args.dataset_version}  samples={len(dataset)}")

    # Train + merge + upload + register
    result = run_training(
        model_id=args.model_id,
        dataset=dataset,
        output_dir=args.output_dir,
        dataset_version=args.dataset_version,
        lora_r=args.lora_r,
        lora_alpha=args.lora_alpha,
        learning_rate=args.lr,
        max_steps=args.max_steps,
        registered_model_name=args.registered_model_name,
        resume_adapter_version=args.resume_from_version,
    )

    # Evaluate + log scores back into the same MLflow run
    if not args.skip_eval:
        scores = run_evaluation(
            model_path=os.path.join(args.output_dir, "adapter"),
            base_model_id=args.model_id,
            eval_data=get_eval_data(),
            run_id=result["run_id"],
        )
        result["eval_scores"] = scores

    # Summary
    print("\n" + "=" * 50)
    print(f"  run_id       : {result['run_id']}")
    print(f"  train_loss   : {result['train_loss']:.4f}")
    print(f"  artifact_uri : {result['artifact_uri']}")
    if result["registry_version"]:
        print(f"  model_name   : {args.registered_model_name}")
        print(f"  version      : {result['registry_version']}")
    if "eval_scores" in result:
        s = result["eval_scores"]
        print(f"  rouge1       : {s['rouge1']:.4f}")
        print(f"  rougeL       : {s['rougeL']:.4f}")
        print(f"  bleu         : {s['bleu']:.4f}")
    print("=" * 50)
    if result["registry_version"]:
        print(f"\nNext: python -m promote --model-name {args.registered_model_name} --version {result['registry_version']}")
    else:
        print("\nNote: pass --registered-model-name to register and promote this run.")
