from datetime import timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pendulum

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'retries': 0,
    'execution_timeout': timedelta(minutes=5),
    'start_date': pendulum.datetime(2025, 1, 1, tz='Asia/Ho_Chi_Minh'),
}

dag = DAG(
    'test_gpu',
    default_args=default_args,
    description='Test GPU availability in Airflow scheduler',
    schedule=None,
    catchup=False,
    tags=['test', 'gpu', 'infrastructure'],
)


def check_nvidia_smi():
    """Check if nvidia-smi is accessible."""
    import subprocess

    print("=" * 60)
    print("NVIDIA-SMI CHECK")
    print("=" * 60)

    result = subprocess.run(
        ['nvidia-smi'],
        capture_output=True, text=True, timeout=30,
    )

    if result.returncode != 0:
        print(f"nvidia-smi failed: {result.stderr}")
        raise RuntimeError("nvidia-smi not available")

    print(result.stdout)
    return True


def check_cuda_torch():
    """Check PyTorch CUDA availability and run a simple tensor operation."""
    print("=" * 60)
    print("PYTORCH CUDA CHECK")
    print("=" * 60)

    try:
        import torch

        print(f"PyTorch version: {torch.__version__}")
        print(f"CUDA available: {torch.cuda.is_available()}")

        if not torch.cuda.is_available():
            print("WARNING: CUDA not available in PyTorch")
            print("This may be expected if PyTorch was installed without CUDA support")
            return False

        print(f"CUDA version: {torch.version.cuda}")
        print(f"GPU count: {torch.cuda.device_count()}")

        for i in range(torch.cuda.device_count()):
            props = torch.cuda.get_device_properties(i)
            mem_gb = props.total_mem / (1024 ** 3)
            print(f"  GPU {i}: {props.name} ({mem_gb:.1f} GB)")

        # Simple GPU computation test
        print("\nRunning GPU tensor test...")
        x = torch.randn(1000, 1000, device='cuda')
        y = torch.randn(1000, 1000, device='cuda')
        z = torch.matmul(x, y)
        print(f"Matrix multiply result shape: {z.shape}, sum: {z.sum().item():.4f}")
        print("GPU tensor test PASSED")

        return True

    except ImportError:
        print("PyTorch not installed - skipping CUDA check")
        return False


def check_gpu_memory():
    """Check GPU memory usage via pynvml."""
    print("=" * 60)
    print("GPU MEMORY CHECK")
    print("=" * 60)

    try:
        import subprocess
        result = subprocess.run(
            ['nvidia-smi', '--query-gpu=index,name,memory.total,memory.used,memory.free,utilization.gpu',
             '--format=csv,noheader,nounits'],
            capture_output=True, text=True, timeout=30,
        )

        if result.returncode != 0:
            raise RuntimeError(f"nvidia-smi query failed: {result.stderr}")

        for line in result.stdout.strip().split('\n'):
            idx, name, total, used, free, util = [x.strip() for x in line.split(',')]
            print(f"GPU {idx}: {name}")
            print(f"  Memory: {used} MB / {total} MB (free: {free} MB)")
            print(f"  Utilization: {util}%")

        return True

    except Exception as e:
        print(f"GPU memory check failed: {e}")
        raise


task_nvidia_smi = PythonOperator(
    task_id='check_nvidia_smi',
    python_callable=check_nvidia_smi,
    dag=dag,
)

task_cuda_torch = PythonOperator(
    task_id='check_cuda_torch',
    python_callable=check_cuda_torch,
    dag=dag,
)

task_gpu_memory = PythonOperator(
    task_id='check_gpu_memory',
    python_callable=check_gpu_memory,
    dag=dag,
)

task_nvidia_smi >> [task_cuda_torch, task_gpu_memory]
