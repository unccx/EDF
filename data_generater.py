import scheduler as sc
import numpy as np
import itertools
from logger_config import logger

np.random.seed(42) 

class DataGenerator(object):
    def __init__(self):
        self.processors = []    # 生成的处理器序列，按照 speed 降序排序
        self.tasks: np.ndarray
        self.hyperedges = []

    def generate_platform(self, processors_number, speed_normalization=False):
        """随机生成一组速度不同的异构处理器平台"""
        for i in range(0, processors_number):
            processor = sc.Processor(id=f"P{i}", speed=np.random.randint(1, 10))
            self.processors.append(processor)
        
        # 打印生成的处理器序列
        self.processors.sort(key=lambda processor : processor.speed, reverse=True) # 将处理器按照 speed 进行降序排序
        fastest_processor_speed = self.processors[0].speed
        if speed_normalization:
            platform_speed = [processor.speed / fastest_processor_speed for processor in self.processors] # 处理器速度归一化
        else:
            platform_speed = [processor.speed for processor in self.processors]
        logger.info(f"platform speed: {platform_speed}")

        return self.processors

    def generate_task(self, number_of_tasks, utilization_normalization=False):

        # 生成 number_of_tasks 个二元组
        binaries = np.random.randint(1, 50, size=(number_of_tasks, 2))

        # 重复最后一列加到最后，即三元组中的deadline == period
        triplets = np.hstack((binaries, binaries[:, -1].reshape(-1, 1)))

        # # 筛选出第二个元素不大于第三个元素的三元组
        # mask = triplets[:, 1] <= triplets[:, 2]
        # triplets = triplets[mask]

        self.tasks = triplets

        # 打印生成的任务
        logger.info("Generated tasks:")
        for i, triplet in enumerate(triplets):
            e, d, T = triplet
            logger.info(f"task{i}: ({e}, {d}, {T})")

        return triplets

    def judge_feasibility(self, task_id_set: list):
        # 任务集为空，不需要调度
        if not task_id_set:
            print("task_id_set is empty and does not need to be scheduled")
            return

        # 判断处理器平台是否为空，为空则需要生成处理器平台
        if not self.processors:
            print("processors is empty, need to generate processor platform")
            return 
        
        scheduler = sc.Scheduler(self.processors)

        # 把 task_id_set 中的 task_id 对应的 task 添加到 scheduler 中
        for task_id in task_id_set:
            e, d, T = self.tasks[task_id, :]
            task = sc.Task(task_id, arrival_timepoint=0, execution_time=e, deadline=d, period=T)
            scheduler.add_task(task)

        # 打印需要判定的待定超边
        logger.info(f"Determining if schedulable: {task_id_set}")

        # 模拟调度过程判断任务集是否可调度
        feasible: bool = scheduler.run(enable_history=False)

        # 打印调度可行性结果
        logger.info(f"feasible: {feasible}")

        return feasible

    def generate_hyperedge(self, max_hyperedge_size=None):
        """为节点集合充分的生成超边
        保存在self.hyperedges中
        """

        # 若超边最大尺寸没有设置或大于节点数量，则设为节点数量（任务数量）
        number_of_tasks = self.tasks.shape[0]
        if not max_hyperedge_size or max_hyperedge_size > number_of_tasks:
            max_hyperedge_size = number_of_tasks 

        task_id_set = list(range(number_of_tasks))
        combin = itertools.combinations(task_id_set, max_hyperedge_size)
        for subset in combin:
            self.search_hyperedge(subset)

        return self.hyperedges


    def search_hyperedge(self, task_id_set):
        """从一组任务节点中递归地找出所有的超边"""
        # 判断 task_id_set 是否为空
        if not task_id_set:
            return

        # 判断 task_set 在 processors 这个平台上是否可调度
        if self.judge_feasibility(task_id_set):
            self.hyperedges.append(task_id_set)
            return 
        else:
            # 任务集不可调度，作为负采样
            pass

        # task_id_set 在 processors 上不可调度，判断 task_id_set 的子集是否可调度
        combin = itertools.combinations(task_id_set, len(task_id_set)-1)
        for subset in combin:
            self.search_hyperedge(subset)