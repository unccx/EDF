import scheduler as sc
import gantt
import data_generater as data
from logger_config import logger

if __name__ == "__main__":
    seed = 1026
    number_of_processors = 9
    number_of_tasks = 1000
    max_hyperedge_size = 7

    # 若超边最大尺寸没有设置或大于节点数量，则以节点数量（任务数量）为最大超边尺寸
    if not max_hyperedge_size or max_hyperedge_size > number_of_tasks:
        data_folder_path = f"./data/data_s{seed}_p{number_of_processors}_t{number_of_tasks}"
    else:
        data_folder_path = f"./data/data_s{seed}_p{number_of_processors}_t{number_of_tasks}_hs{max_hyperedge_size}"
    dg = data.DataGenerator(seed, data_folder_path)
    platform = dg.generate_platform(number_of_processors)
    task_set = dg.generate_tasks(number_of_tasks)

    dg.generate_hyperedge(max_hyperedge_size)
