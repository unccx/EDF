import scheduler as sc
import gantt
import data_generater as data
import logger_config
from pathlib import Path

if __name__ == "__main__":
    seed = 3000
    number_of_processors = 5
    number_of_tasks = 3000
    max_hyperedge_size = 10
    num_of_hyperedge = 10000

    data_folder_path = Path(f"./data/data_s{seed}_p{number_of_processors}_t{number_of_tasks}_hs{max_hyperedge_size}_e{num_of_hyperedge}")
    dg = data.DataGenerator(seed, data_folder_path)
    platform = dg.generate_platform(number_of_processors)
    task_set = dg.generate_tasks(number_of_tasks)

    dg.generate_hyperedge(max_hyperedge_size, num_of_hyperedge)
