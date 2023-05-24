import matplotlib.pyplot as plt

def plot_gantt(scheduler):
    # Declaring a figure
    fig, ax = plt.subplots()

    # 设置坐标轴
    ax.set_ylim(0, len(scheduler.processors) + 15)
    ax.set_xlabel('Time')
    ax.set_yticks([i*10 + 2 for i in range(len(scheduler.processors))])
    ax.set_yticklabels([processor.id for processor in scheduler.processors])

    # Setting graph attribute
    ax.grid(True)

    # 绘制条形图，并将任务序号写在矩形中间
    for i, processor in enumerate(scheduler.processors):
        # 绘制代表processor这一行的running history
        for record in processor.history:
            task_id, task_instance_id = record[0:2]
            color = 'C' + str(task_id % 10) 
            ax.broken_barh([record[2:4]], (i*10, 4), facecolors=color)
            ax.annotate(f"t{task_id},{task_instance_id}", 
                        xy=(record[2]+record[3]/2, i*10+2), ha='center', va='center')

    plt.savefig("gantt1.png")
