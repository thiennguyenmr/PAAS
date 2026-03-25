"""
evaluate_model_v1.py — Standalone evaluation + MLflow integration.

Usage:
    from evaluation.evaluate_model_v1 import run_evaluation
    scores = run_evaluation(model_path="./outputs/run1/merged", run_id="<run_id>")
"""

import os
import mlflow
import torch
import evaluate as hf_evaluate
from peft import PeftModel
from transformers import AutoTokenizer, AutoModelForCausalLM

import mlflow_client


def run_evaluation(
    model_path: str,
    eval_data: list[dict],  # [{"prompt": str, "reference": str}]
    run_id: str = None,
    base_model_id: str = None,  # required when model_path is a LoRA adapter
) -> dict:
    """
    Generate responses for each eval prompt, compute ROUGE + BLEU,
    and optionally log scores into an existing MLflow run.

    Supports two loading modes:
      - LoRA adapter: model_path contains adapter_config.json
                      → loads base_model_id + PeftModel → merge_and_unload()
      - Full model  : loads model_path directly

    Returns:
        {"rouge1": float, "rouge2": float, "rougeL": float, "bleu": float}
    """
    is_adapter = os.path.isfile(os.path.join(model_path, "adapter_config.json"))

    if is_adapter:
        if not base_model_id:
            raise ValueError("base_model_id is required when model_path is a LoRA adapter")
        print(f"[Eval] Loading base '{base_model_id}' + adapter '{model_path}' ...")
        load_id = base_model_id
    else:
        print(f"[Eval] Loading full model '{model_path}' ...")
        load_id = model_path

    tokenizer = AutoTokenizer.from_pretrained(load_id, trust_remote_code=True)
    tokenizer.pad_token = tokenizer.eos_token
    base = AutoModelForCausalLM.from_pretrained(
        load_id,
        torch_dtype=torch.float16,
        device_map="auto",
        trust_remote_code=True,
    )
    model = PeftModel.from_pretrained(base, model_path).merge_and_unload() if is_adapter else base
    model.eval()

    predictions = []
    references = []

    for sample in eval_data:
        inputs = tokenizer(sample["prompt"], return_tensors="pt").to(model.device)
        with torch.no_grad():
            output_ids = model.generate(
                **inputs,
                max_new_tokens=100,
                pad_token_id=tokenizer.eos_token_id,
            )
        # Decode only the newly generated tokens
        new_tokens = output_ids[0][inputs["input_ids"].shape[1]:]
        predictions.append(tokenizer.decode(new_tokens, skip_special_tokens=True).strip())
        references.append(sample["reference"])

    rouge = hf_evaluate.load("rouge")
    bleu  = hf_evaluate.load("bleu")

    rouge_results = rouge.compute(predictions=predictions, references=references)
    bleu_results  = bleu.compute(predictions=predictions, references=[[r] for r in references])

    scores = {
        "rouge1": round(rouge_results["rouge1"], 4),
        "rouge2": round(rouge_results["rouge2"], 4),
        "rougeL": round(rouge_results["rougeL"], 4),
        "bleu":   round(bleu_results["bleu"],    4),
    }
    print(f"[Eval] {scores}")

    if run_id:
        with mlflow.start_run(run_id=run_id):
            mlflow_client.log_metrics(scores)
        print(f"[Eval] Logged to run {run_id}")

    return scores
