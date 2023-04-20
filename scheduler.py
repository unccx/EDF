import heapq
import math

class Task(object):
    '''周期任务
    可由(e, d, T)所刻画，其他属性例如arrival_timepoint、remaining_time使得周期任务在模拟时循环按周期到达
    Attributes:
        id:                 # 任务id
        arrival_timepoint:  # 到达时刻
        instance_id         # 任务实例id，也表示该任务到达次数
        remaining_time:     # 剩余执行工作量时间，也是在最慢处理器上测量得到
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
        if self.remaining_time == 0:
            self.arrival_timepoint = self.arrival_timepoint + self.period
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
                        元素为(task.id, task.instance_id)的二元组，若处理器idle则为None
    '''
    def __init__(self, id, speed):
        self.id = id                # 处理器id
        self.speed = speed          # 性能
        self.end_timepoint = None   # 处理器所分配任务的执行结束时刻
        self.current_task = None    # 处理器上当前所分配任务
        self.history = []           # 在此处理器上的运行历史

    def __lt__(self, other):
        return self.end_timepoint < other.end_timepoint

    def assign_task(self, task, current_timepoint):
        self.current_task = task
        # 分配任务时，计算新分配的任务在此处理器上什么时候能够完成
        self.end_timepoint = self.current_task.remaining_time // self.speed + current_timepoint

    def detach_task(self):
        '''去除处理器上所执行的任务
        '''
        self.current_task = None
        self.end_timepoint = None

    def execute_task(self, run_time=1):
        '''任务执行
        run_time:
            值得注意的是，若run_time>1，在执行任务的这run_time个单位时间内，任务结束后也不会继续给处理器分配任务
            建议在调用execute_tasks时，run_time设置为current_timepoint与next schedule event timepoint的间隔
            用于快速跳至next schedule event timepoint
            或者run_time=1，Scheduler.run()中的current_timepoint每次+1，不去节省模拟没有schedule event的时间
        '''
        if self.current_task:
            self.current_task.remaining_time -= self.speed * run_time
            self.history.append((self.current_task.id, self.current_task.instance_id))
            # 任务执行结束
            if self.current_task.remaining_time <= 0:
                self.current_task.renew()
                self.current_task = None
                self.end_timepoint = None
        else:
            # 处理器idle
            self.history.append(None)

class Scheduler(object):
    def __init__(self, processors, current_timepoint = 0):
        self.tasks = []
        self.processors = processors
        self.current_timepoint = current_timepoint
        self.task_deadline_heap = []                # 用优先队列表示任务集tasks的优先级
        self.lcm_period = 1                         # 任务集tasks的周期的最小公倍数

    def add_task(self, task):
        '''添加任务，同时计算任务集中任务的期限的最小公倍数
        '''
        self.tasks.append(task)
        self.lcm_period = math.lcm(self.lcm_period, task.period)

    def priority_tick(self):
        '''更新任务优先级
        '''
        self.task_deadline_heap.clear()
        for task in self.tasks:
            if task.arrival_timepoint <= self.current_timepoint and task.remaining_time > 0:
                heapq.heappush(self.task_deadline_heap, task)
        
        # 打印self.task_deadline_heap中的任务id
        tasks_priority = [task.id for task in self.task_deadline_heap]
        print(f"tasks priority:{tasks_priority}")

    def allocation_tick(self):
        '''更新任务分配到处理器上的情况
        '''
        for processor in self.processors:
            if not processor.current_task:
                # 将最高优先级的任务分配到最快的processor上
                if self.task_deadline_heap:
                    task = heapq.heappop(self.task_deadline_heap)
                    processor.assign_task(task, self.current_timepoint)
        
        # 打印处理器分配情况
        processor_allocation = [(processor.id, processor.current_task.id) for processor in self.processors if processor.current_task != None]
        print(f"Processor Allocation:{processor_allocation}")

    def run(self):
        '''模拟对任务集进行调度
        返回一个布尔值，可实时调度为True，不可实时调度为False
        '''
        while self.current_timepoint <= self.lcm_period:
            print(f"[{self.current_timepoint}]:")

            # 检查是否存在任务已超出期限
            for task in self.tasks:
                if self.current_timepoint >= task.abs_deadline:
                    return False

            # 更新所有活跃任务（已到达并且未执行完毕）的优先级
            self.priority_tick()

            # 分配任务到处理器，processors的排序即为任务分配的顺序
            self.allocation_tick()

            # 执行所有处理器上的任务
            for processor in self.processors:
                processor.execute_task()
                processor.detach_task()

            # Update time
            self.current_timepoint += 1
        else:
            return True