"""
Dummy training/evaluation datasets.

Two dataset versions (v1, v2) with 3 English Q&A samples each.
Format: { "text": "<full_prompt_with_instruction_template>" }

Kept simple — no domain-specific content required.
"""

from datasets import Dataset

# ── Instruction template (Llama-3 chat format) ──────────────────────────────

SYSTEM_MSG = "You are a helpful assistant. Answer the user's question concisely."


def _fmt(question: str, answer: str) -> str:
    return (
        "<|begin_of_text|><|start_header_id|>system<|end_header_id|>\n"
        f"{SYSTEM_MSG}\n"
        "<|eot_id|><|start_header_id|>user<|end_header_id|>\n"
        f"{question}\n"
        "<|eot_id|><|start_header_id|>assistant<|end_header_id|>\n"
        f"{answer}\n"
        "<|eot_id|>"
    )


# ── Version 1 ────────────────────────────────────────────────────────────────

DATASET_V1 = [
    {
        "text": _fmt(
            "What is the capital of France?",
            "The capital of France is Paris.",
        )
    },
    {
        "text": _fmt(
            "What is 2 + 2?",
            "2 + 2 equals 4.",
        )
    },
    {
        "text": _fmt(
            "What color is the sky on a clear day?",
            "The sky is blue on a clear day.",
        )
    },
]

# ── Version 2 (different questions, slightly more detail in answers) ──────────

DATASET_V2 = [
    {
        "text": _fmt(
            "What is the largest planet in our solar system?",
            "Jupiter is the largest planet in our solar system.",
        )
    },
    {
        "text": _fmt(
            "What is the chemical formula for water?",
            "The chemical formula for water is H2O.",
        )
    },
    {
        "text": _fmt(
            "Who wrote the play Romeo and Juliet?",
            "Romeo and Juliet was written by William Shakespeare.",
        )
    },
]

# ── Evaluation set (prompt + reference, used in evaluate_model_v1.py) ────────

EVAL_DATA = [
    {
        "prompt": "What is the capital of France?",
        "reference": "The capital of France is Paris.",
    },
    {
        "prompt": "What is 2 + 2?",
        "reference": "2 + 2 equals 4.",
    },
    {
        "prompt": "What is the largest planet in our solar system?",
        "reference": "Jupiter is the largest planet in our solar system.",
    },
]


def get_train_dataset(version: str = "v1") -> Dataset:
    """Return HuggingFace Dataset for the given version ('v1' or 'v2')."""
    data = DATASET_V1 if version == "v1" else DATASET_V2
    return Dataset.from_list(data)


def get_eval_data() -> list[dict]:
    """Return raw eval list: [{'prompt': str, 'reference': str}, ...]."""
    return EVAL_DATA
