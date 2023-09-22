import scheduler as sc
import numpy as np
import itertools
import csv
import os
from logger_config import logger
from pathlib import Path
from tqdm import tqdm
import math
import random

class DataGenerator(object):
    def __init__(self, seed, data_path:Path):
        self.processors = []        # 生成的处理器序列，按照 speed 降序排序
        self.tasks: np.ndarray      # tasks是一个 4 * number_of_tasks 的二维数组，row_index为task_id, 
        self.hyperedges: set = set()
        self.negative_samples: set = set()
        self.minimal_unschedulable_combinations: set = set()
        self.data_path = data_path
        np.random.seed(seed)

        # 如果data目录不存在，则创建它
        if not os.path.exists(data_path):
            os.makedirs(data_path)

        # 清除 data 目录下的 hyperedges.csv 和 negative_samples.csv
        hyperedges_file = data_path / "hyperedges.csv"
        negative_samples_file = data_path / "negative_samples.csv"
        if os.path.exists(hyperedges_file):
            os.remove(hyperedges_file)
        if os.path.exists(negative_samples_file):
            os.remove(negative_samples_file)

    def generate_platform(self, processors_number: int, speed_normalization: bool=False):
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

        self.save_platform(self.data_path / "platform.csv")

        return self.processors

    def generate_tasks(self, number_of_tasks):
        if not self.processors:
            logger.error(f"platform not generated")

        # 随机生成一列归一化利用率
        utilization_col = np.random.rand(number_of_tasks).reshape(-1, 1)
        utilization_col = np.sort(utilization_col, axis=0) # 对生成的利用率排序

        # 随机生成一列deadline
        deadline_col = np.random.randint(1, 100, size=number_of_tasks).reshape(-1, 1)

        # # 随机生成一列period，需要 period >= deadline
        # period_col = np.random.randint(1, 100, size=(number_of_tasks, 1)) + deadline_col
        # 随机生成一列period，需要 period == deadline
        period_col = np.random.randint(0, 1, size=(number_of_tasks, 1)) + deadline_col

        # 根据周期和归一化利用率计算出在最慢处理器上测量的执行时间。
        fastest_processor_speed = self.processors[0].speed # self.processors按照 speed 降序排序
        exec_time_col = period_col * utilization_col * fastest_processor_speed
        exec_time_col = np.ceil(exec_time_col)

        quadruples = np.hstack((exec_time_col, deadline_col, period_col, utilization_col))

        self.tasks = quadruples

        # 打印生成的任务
        logger.info("Generated tasks:")
        for i, quadruple in enumerate(quadruples):
            e, d, T, u = quadruple
            logger.info(f"task{i}: \t({e},\t{d},\t{T},\t{u :.2f})")

        # 保存tasks
        self.save_tasks(self.data_path / "task_quadruples.csv")

        return quadruples
    
    # 保存数据
    def save_tasks(self, file_name:Path=Path("data/task_quadruples.csv")):
        np.savetxt(file_name, self.tasks, delimiter=",")

    def save_platform(self, file_name:Path=Path("data/platform.csv")):
        with open(file_name, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            for processor in self.processors:
                writer.writerow([processor.id, processor.speed])

    def save_hyperedges(self, file_name:Path=Path("data/hyperedges.csv")):
        with open(file_name, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            for hyperedge in self.hyperedges:
                writer.writerow([str(node_id) for node_id in hyperedge])

    def save_negative_samples(self, file_name:Path=Path("data/negative_samples.csv")):
        with open(file_name, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            for negative_sample in self.negative_samples:
                writer.writerow([str(node_id) for node_id in negative_sample])

    def calculate_system_utilization(self, task_id_set):
        """计算系统利用率"""
        task_id_set = list(task_id_set)
        sum_of_normalized_utilization = sum(self.tasks[task_id_set][:, 3])

        # 求处理器归一化后，所有处理器性能之和 S_m
        fastest_processor_speed = self.processors[0].speed
        platform_speed = [processor.speed / fastest_processor_speed for processor in self.processors] # 处理器速度归一化
        sum_of_normalized_speed = sum(platform_speed) # S_m

        return sum_of_normalized_utilization / sum_of_normalized_speed
    
    def has_unschedulable_combination(self, task_id_set):
        """检查任务集是否含有不可调度组合，如果存在返回True，否则False"""
        for combin in self.minimal_unschedulable_combinations:
            if combin.issubset(task_id_set):
                return True # 包含不可调度组合
        return False

    def judge_feasibility(self, task_id_set) -> bool:
        # 任务集为空，不需要调度
        if not task_id_set:
            logger.warning("task_id_set is empty and does not need to be scheduled")
            return False

        # 判断处理器平台是否为空，为空则需要生成处理器平台
        if not self.processors:
            logger.error("processors is empty, need to generate processor platform")
            return False
        
        scheduler = sc.Scheduler(self.processors)

        # 把 task_id_set 中的 task_id 对应的 task 添加到 scheduler 中
        for task_id in task_id_set:
            e, d, T, _ = self.tasks[task_id, :]
            task = sc.Task(task_id, arrival_timepoint=0, 
                           execution_time=int(e), deadline=int(d), period=int(T))
            scheduler.add_task(task)

        # 模拟调度过程判断任务集是否可调度
        feasible: bool = scheduler.run(truncated_lcm=1000000, enable_history=False)

        return feasible

    def generate_hyperedge(self, max_hyperedge_size=None, num_of_hyperedge=None):
        """为节点集合充分的生成超边
        保存在self.hyperedges中
        """

        # 若超边最大尺寸没有设置或大于节点数量，则设为节点数量（任务数量）
        number_of_tasks = self.tasks.shape[0]
        if not max_hyperedge_size or max_hyperedge_size > number_of_tasks:
            max_hyperedge_size = number_of_tasks 

        if not num_of_hyperedge:
            num_of_hyperedge = number_of_tasks * max_hyperedge_size

        for i in tqdm(range(num_of_hyperedge), desc="search hyperedge", total=num_of_hyperedge):
            task_id_set = random.sample(range(number_of_tasks), max_hyperedge_size)
            self.search_hyperedge(frozenset(task_id_set))

        return self.hyperedges


    def search_hyperedge(self, task_id_set: frozenset) -> bool:
        """从一组任务节点中递归地找出所有的超边"""

        # 判断 task_id_set 是否为空
        if not task_id_set:
            return True
        
        # 判断任务集是否已经判定过可调度性，如果已经搜索过则不再往下搜索
        if task_id_set in self.hyperedges:
            return True
        if task_id_set in self.negative_samples:
            return False
        
        # 检查 task_id_set 是否为已判定为可调度的任务集的子集
        for hyperedge in self.hyperedges:
            if hyperedge.issuperset(task_id_set):
                # 这里的可调度超边为之前经历模拟验证的可调度超边的子集，因此被剪枝。在其他分支中又出现需要判断可调度性，所以不用记录为超边
                return True
        
        # 先判断是否满足系统利用率小于等于1的可调度性必要条件，
        # 然后判断任务集中是否包含不可调度组合
        # 最后通过模拟验证 task_set 在 processors 这个平台上是否可调度
        system_utilization = self.calculate_system_utilization(task_id_set)
        if (system_utilization <= 1                                     # 不满足必要条件，也要继续往下搜索子集可调度性
            and (not self.has_unschedulable_combination(task_id_set))   # 发现含有不可调度组合直接判定任务集不可调度，也要继续往下搜索子集
            and self.judge_feasibility(task_id_set)):
            # 记录已搜索过的超边
            self.hyperedges.add(task_id_set)

            # 保存超边
            file_name = self.data_path / "hyperedges.csv"
            with open(file_name, 'a', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow([str(node_id) for node_id in sorted(task_id_set)])
            logger.info(f"feasible: True \tsystem utilization: {system_utilization * 100 :.2f}%\ttask set: {task_id_set}") # 打印调度可行性结果

            return True
        else: # task_id_set 不可调度，搜索子集
            # 记录已搜索过的负采样
            self.negative_samples.add(task_id_set)

            # 保存负采样
            file_name = self.data_path / "negative_samples.csv"
            with open(file_name, 'a', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow([str(node_id) for node_id in sorted(task_id_set)])
            logger.info(f"feasible: False \tsystem utilization: {system_utilization * 100 :.2f}%\ttask set: {task_id_set}") # 打印调度可行性结果

            # 走到这一步，说明 task_id_set 在 processors 上不可调度，判断 task_id_set 的子集是否可调度
            combin = itertools.combinations(task_id_set, len(task_id_set)-1)
            flag: bool = True # 记录所有真子集是否都是可调度的
            for subset in combin:
                flag = self.search_hyperedge(frozenset(subset)) and flag # 真子集全部可调度，flag为True；存在真子集不可调度，flag为False

            # 当 task_id_set 的真子集都可调度且 task_id_set 不可调度时，说明 task_id_set 是一个会导致任务集不可调度的最小任务组合，任务集中存在这个组合即不可调度
            if flag: 
                self.minimal_unschedulable_combinations.add(task_id_set)
                file_name = self.data_path / "minimal_unschedulable_combinations.csv"
                with open(file_name, 'a', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow([str(node_id) for node_id in sorted(task_id_set)])

            return False