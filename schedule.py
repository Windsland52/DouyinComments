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

# 每天早上7:30执行任务
scheduler.add_job(scheduled_task, 'cron', hour=7, minute=30)

# 每天中午12:00执行任务
scheduler.add_job(scheduled_task, 'cron', hour=11, minute=30)

# 每天下午6:00执行任务
scheduler.add_job(scheduled_task, 'cron', hour=17, minute=30)

# 每天晚上8:00执行任务
scheduler.add_job(scheduled_task, 'cron', hour=19, minute=30)

# 启动调度器
try:
    logging.info("调度器开始运行...")
    scheduler.start()
except (KeyboardInterrupt, SystemExit):
    pass
