import scheduler as sc
import gantt
import data_generater as data
from logger_config import logger

def main():
    dg = data.DataGenerator()
    platform = dg.generate_platform(5)
    task_set = dg.generate_tasks(11)

    dg.generate_hyperedge()
    # dg.save_hyperedges()
    # dg.save_negative_samples()

if __name__ == "__main__":
    main()