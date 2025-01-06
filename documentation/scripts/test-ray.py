import ray

ray.init(address='auto')

@ray.remote
def task():
    return "Task completed!"

futures = [task.remote() for _ in range(5)]
print(ray.get(futures))

