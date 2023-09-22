import data_generater as data
from pathlib import Path
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--seed', help='设置随机种子', type=int, required=True)
    parser.add_argument('-p', '--number_of_processors', help='设置处理器数量', type=int, required=True)
    parser.add_argument('-t', '--number_of_tasks', help='设置任务数量', type=int, required=True)
    parser.add_argument('-hs', '--max_hyperedge_size', help='设置超边最多可链接的节点数量', type=int)
    parser.add_argument('-e', '--num_of_hyperedge', help='设置随机搜索的超边数量', type=int, required=True)

    args = parser.parse_args()

    seed = args.seed
    number_of_processors = args.number_of_processors
    number_of_tasks = args.number_of_tasks
    max_hyperedge_size = args.max_hyperedge_size if args.max_hyperedge_size else args.number_of_tasks
    num_of_hyperedge = args.num_of_hyperedge

    data_folder_path = Path(f"./data/data_s{seed}_p{number_of_processors}_t{number_of_tasks}_hs{max_hyperedge_size}_e{num_of_hyperedge}")
    dg = data.DataGenerator(seed, data_folder_path)
    platform = dg.generate_platform(number_of_processors)
    task_set = dg.generate_tasks(number_of_tasks)

    dg.generate_hyperedge(max_hyperedge_size, num_of_hyperedge)
