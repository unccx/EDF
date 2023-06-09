import heapq
import math
from logger_config import logger
from tqdm import tqdm

class Task(object):
    '''周期任务
    可由(e, d, T)所刻画，其他属性例如arrival_timepoint、remaining_time使得周期任务在模拟时循环按周期到达
    Attributes:
        id:                 # 任务id
        arrival_timepoint:  # 到达时刻
        instance_id         # 任务实例id，也表示该任务到达次数
        remaining_time:     # 剩余执行工作量时间，也是在最慢处理器上测量得到
        abs_deadline        # 绝对期限
        execution_time:     # e 在最慢的速度为1的处理器上测量的执行时间
        deadline:           # d 相对期限
        period:             # T 周期
    '''
    def __init__(self, id, arrival_timepoint, execution_time, deadline, period):
        self.id = id                                # 任务id
        self.arrival_timepoint = arrival_timepoint  # 到达时刻
        self.instance_id = 0                        # 任务实例id，也表示该任务到达次数
        self.remaining_time = execution_time        # 剩余执行工作量时间，也是在最慢处理器上测量得到
        self.abs_deadline = deadline                # 绝对期限
        self.execution_time = execution_time        # 在最慢的速度为1的处理器上测量的执行时间
        self.deadline = deadline                    # 相对期限
        self.period = period                        # 周期

    def __lt__(self, other):
        '''任务优先级'''
        return self.abs_deadline < other.abs_deadline

    def renew(self):
        '''在任务结束时，使任务按周期更新下一到达时刻'''
        if self.remaining_time <= 0:
            self.arrival_timepoint += self.period
            self.instance_id += 1
            self.remaining_time = self.execution_time
            self.abs_deadline += self.period

class Processor(object):
    '''处理器
    Attributes:
        id:             处理器id
        speed:          性能
        end_timepoint:  处理器所分配任务的执行结束时刻
                        可以作为对Processors排序的依据，选出最早完成任务的处理器，将当前时间跳到next schedule event timepoint
        current_task:   处理器上当前所分配任务
        history:        在此处理器上的运行历史
                        元素为(task.id, task.instance_id, current_timepoint, running_time)的四元组
    '''
    def __init__(self, id, speed, enable_history=True):
        self.id = id                            # 处理器id
        self.speed = speed                      # 性能
        self.end_timepoint = None               # 处理器所分配任务的执行结束时刻
        self.current_task = None                # 处理器上当前所分配任务
        self.history = []                       # 在此处理器上的运行历史

    def __lt__(self, other):
        return self.end_timepoint < other.end_timepoint

    def assign_task(self, task, current_timepoint):
        self.current_task = task
        # 分配任务时，计算新分配的任务在此处理器上什么时候能够完成
        # 这里使用向上取整除法math.ceil()，是因为如果这里向下取整可能会使
        # ==>   remaining_time // speed == 0
        # ==>   end_timepoint == current_timepoint
        # ==>   next_schedule_event_timepoint = current_timepoint
        # ==>   running_time == 0，出现死循环
        self.end_timepoint = math.ceil(self.current_task.remaining_time / self.speed) + current_timepoint

    def detach_task(self):
        '''去除处理器上所执行的任务'''
        self.current_task = None
        self.end_timepoint = None

    def execute_task(self, current_timepoint, running_time=1, enable_history=True):
        '''任务执行
        running_time:
            值得注意的是，若running_time>1，在执行任务的这running_time个单位时间内，任务结束后也不会继续给处理器分配任务
            建议在调用execute_tasks时，running_time设置为current_timepoint与next schedule event timepoint的间隔
            用于快速跳至next schedule event timepoint
            或者running_time=1，Scheduler.run()中的current_timepoint每次+1，不去节省模拟没有schedule event的时间
        enable_history:
            启用处理器执行历史记录。如果需要gantt图可视化调度过程，需要为True
        '''
        if self.current_task:
            self.current_task.remaining_time -= self.speed * running_time

            if enable_history:
                # 记录执行历史
                self.history.append((self.current_task.id, self.current_task.instance_id, current_timepoint, running_time))

            # 任务执行结束
            if self.current_task.remaining_time <= 0:
                # 打印任务结束时刻
                logger.debug(f"task {self.current_task.id} is completed at timepoint {current_timepoint + running_time}")

                self.current_task.renew()
                self.current_task = None
                self.end_timepoint = None

class Scheduler(object):
    def __init__(self, processors, current_timepoint = 0):
        # 将处理器按照speed进行降序排序
        processors.sort(key=lambda processor : processor.speed, reverse=True)

        self.tasks = []
        self.processors = processors                # 处理器list
        self.current_timepoint = current_timepoint
        self.task_deadline_heap = []                # 用优先队列表示任务集tasks的优先级
                                                    # 因为在分配任务时会从优先队列中取出优先级最高的任务，所以不能代表活跃任务
                                                    # task_deadline_heap中的任务 + 处理器上运行的任务 = 活跃任务
        self.lcm_period = 1                         # 任务集tasks的周期的最小公倍数

    def add_task(self, task):
        '''添加任务，同时计算任务集中任务的期限的最小公倍数'''
        self.tasks.append(task)
        self.lcm_period = math.lcm(self.lcm_period, task.period)

    def priority_tick(self):
        '''更新任务优先级'''
        self.task_deadline_heap.clear()
        for task in self.tasks:
            if task.arrival_timepoint <= self.current_timepoint and task.remaining_time > 0:
                heapq.heappush(self.task_deadline_heap, task)

                # 打印任务到达时刻
                if task.arrival_timepoint == self.current_timepoint:
                    logger.debug(f"task {task.id} arrives at time {task.arrival_timepoint}")
        
        # 打印self.task_deadline_heap中的任务id
        # 使用[task.abs_deadline for task in self.task_deadline_heap]可打印出绝对deadline
        tasks_priority = [task.id for task in self.task_deadline_heap]
        logger.debug(f"tasks priority:{tasks_priority}")

    def allocation_tick(self):
        '''更新任务分配到处理器上的情况'''
        # 清除上一tick的处理器分配情况
        for processor in self.processors:
            processor.detach_task()

        # 这一tick重新将最高优先级的任务分配到最快的processor上，处理器list在init()中按照处理器速度进行过降序排序
        for processor in self.processors:
            # 处理器空闲并且任务队列非空
            if not processor.current_task and self.task_deadline_heap:
                task = heapq.heappop(self.task_deadline_heap)
                processor.assign_task(task, self.current_timepoint)
        
        # 打印处理器分配情况
        processor_allocation = [(processor.id, processor.current_task.id) 
                                for processor in self.processors if processor.current_task != None]
        logger.debug(f"Processor Allocation:{processor_allocation}")

    def run(self, truncated_lcm=-1, enable_history:bool=True):
        '''模拟对任务集进行调度
        返回一个布尔值，可实时调度为True，不可实时调度为False
        enable_history:
            启用处理器执行历史记录。如果需要gantt图可视化调度过程，需要为True
        '''
        if truncated_lcm < 0 or truncated_lcm > self.lcm_period:
            truncated_lcm = self.lcm_period

        with tqdm(total=truncated_lcm) as pbar:
            pbar.set_description('Simulation Processing:')
            while self.current_timepoint <= truncated_lcm:
                # print(f"Simulation progress: [{self.current_timepoint} / {truncated_lcm} = {self.current_timepoint / self.lcm_period * 100 :.2f}%]:", end='', flush=True)
                # print("\r", end='', flush=True)

                # 检查是否存在任务已超出期限
                for task in self.tasks:
                    if self.current_timepoint >= task.abs_deadline:
                        logger.debug(f"task {task.id} exceeded the deadline")
                        return False # 任务集不可调度

                # 更新所有活跃任务（已到达并且未执行完毕的任务）的优先级
                self.priority_tick()

                # 分配任务到处理器，processors的排序即为任务分配的顺序
                self.allocation_tick()

                # 找出触发下一调度事件的时间点，并计算出距离现在还有多少时间
                # Schedule events 包括两种情况，当 (a) 正在运行的作业完成时和 (b) 新作业到达时
                running_task_end = [processor.end_timepoint 
                                    for processor in self.processors if processor.end_timepoint]
                new_arrival = [task.arrival_timepoint for task in self.tasks 
                            if task.arrival_timepoint > self.current_timepoint]
                next_schedule_event_timepoint = min(running_task_end + new_arrival)
                running_time = next_schedule_event_timepoint - self.current_timepoint # simulation step

                # 当 running_time <= 0 时，给出警告
                if running_time <= 0:
                    logger.critical(f"running time(simulation step) <= 0 will cause an infinite loop: running_time={running_time}")

                # 执行所有处理器上的任务
                for processor in self.processors:
                    processor.execute_task(self.current_timepoint, running_time, enable_history)

                # Update time
                self.current_timepoint += running_time
                pbar.update(running_time)
            else:
                # 当存在归一化利用率大于1的任务时，显然不可调度，判断任务已超出期限的时刻在 next_sche_event。
                # 但是当 deadline 到 lcm_period 之间没有调度事件存在时，即 next_sche_event 在 lcm_period 之后，
                # 因为 self.current_timepoint <= self.lcm_period 为 False，循环不再执行。没有判断 next_sche_event
                # 是否有超出期限的任务，导致循环结束直接认为任务集可调度。如下图所示：
                #            ┌────────────────────────────────────────────────────┬────────────────┐
                #            │                                                    │                │
                #            ├──────────────────┬─────────────────────────────────┤                │
                #            │                  │                                 │                ▼
                # first_sche_event(start)   deadline                         lcm_period      next_sche_event
                #            │                                                                     │
                #            │◄──────────────────────running_time(step)───────────────────────────►│
                #            │                                                                     │
                #
                # 因此需要在循环结束时再判断一次是否存在任务已超出期限

                # 检查是否存在任务已超出期限
                for task in self.tasks:
                    if self.current_timepoint >= task.abs_deadline:
                        logger.debug(f"task {task.id} exceeded the deadline")
                        return False # 任务集不可调度

                return True # 任务集可调度