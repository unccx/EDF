import scheduler as sc
import gantt

def main():
    processors = [
        sc.Processor(id='A', speed=1),
        # sc.Processor(id='B', speed=1),
        # sc.Processor(id='C', speed=1),
        # sc.Processor(id='D', speed=1),
    ]

    # # 将处理器按照speed进行降序排序
    # processors.sort(key=lambda processor : processor.speed, reverse=True)

    tasks = [
        sc.Task(1, arrival_timepoint=0, execution_time=25, deadline=50, period=50),
        sc.Task(2, arrival_timepoint=0, execution_time=30, deadline=75, period=75),
        # sc.Task(3, arrival_timepoint=0, execution_time=20, deadline=11, period=11),
        # sc.Task(4, arrival_timepoint=0, execution_time=6, deadline=10, period=10),
        # sc.Task(5, arrival_timepoint=0, execution_time=6, deadline=10, period=10),
    ]

    scheduler = sc.Scheduler(processors)
    for task in tasks:
        scheduler.add_task(task)

    print(f"lcm:{scheduler.lcm_period}")

    feasible = scheduler.run()
    if feasible:
        print("The tasks are feasible")
    else:
        print("The tasks are not feasible")

    for processor in processors:
        print(f"{processor.id}:{processor.history}")

    gantt.plot_gantt(scheduler)

if __name__ == "__main__":
    main()