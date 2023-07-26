import scheduler as sc
import gantt
import data_generater as data
from logger_config import logger

if __name__ == "__main__":
    seed = 2233
    number_of_processors = 10
    number_of_tasks = 21
    data_folder_path = f"./data/data_s{seed}_p{number_of_processors}_t{number_of_tasks}"
    dg = data.DataGenerator(seed, data_folder_path)
    platform = dg.generate_platform(number_of_processors)
    task_set = dg.generate_tasks(number_of_tasks)

    dg.generate_hyperedge()
