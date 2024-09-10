from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
from main import *

# 定义任务
def scheduled_task():
    try:
        main()
        logging.info('Task all done!')
    except Exception as e:
        logging.error(f"An error occurred during the task: {e}", exc_info=True)
    logging.info(f"任务执行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# 创建调度器
scheduler = BlockingScheduler()

# 每天早上7:30执行任务，并设置容错时间为1小时
scheduler.add_job(scheduled_task, 'cron', hour=7, minute=30, misfire_grace_time=3600)

# 每天中午11:30执行任务
scheduler.add_job(scheduled_task, 'cron', hour=11, minute=30, misfire_grace_time=3600)

# 每天下午5:30执行任务
scheduler.add_job(scheduled_task, 'cron', hour=17, minute=30, misfire_grace_time=3600)

# 每天晚上7:30执行任务
scheduler.add_job(scheduled_task, 'cron', hour=19, minute=30, misfire_grace_time=3600)

# 启动调度器
try:
    logging.info("调度器开始运行...")
    scheduler.start()
except (KeyboardInterrupt, SystemExit):
    pass
