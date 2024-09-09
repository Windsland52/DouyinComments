# 抖音评论爬虫监控脚本

本脚本用于爬取指定批量抖音视频的评论及其回复，将结果保存为CSV文件，并监控增量更新的评论。

原项目：[alphaply/DouyinComments](https://github.com/alphaply/DouyinComments)

参考项目：[NanmiCoder/MediaCrawler](https://github.com/NanmiCoder/MediaCrawler)

本项目在原项目基础上进行了修改，特别感谢两位作者。

## 环境要求

脚本运行需要以下环境：
- asyncio
- httpx
- pandas
- execjs
- apscheduler（定时任务用，只在schedule.py中使用）
- nodejs（重要）


## 安装依赖

在运行脚本之前，请确保安装了所有必要的依赖,别忘记安装nodejs：

```bash
pip install httpx pandas PyExecJS apscheduler
```

## 脚本运行

请先到config.py文件中完成配置，详细配置说明请参考config，然后运行main.py文件：

```bash
python main.py
```

## 输出

- data：包含所有评论及其回复的CSV文件，文件名为视频id_comments/replies.csv。增量更新在相应时间的文件夹中。
- logs：日志文件，包含爬取过程中的信息，按日分割。

## 其它功能

1. 定时任务：可以设置定时任务，每天自动爬取指定抖音视频的评论及其回复。

```python
# 按需修改schedule.py后执行
python schedule.py
```

2. 评论记录：可单独存储标记的坏评论，用于日后分析。

```python
python comments.py
```
