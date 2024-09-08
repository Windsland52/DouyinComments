# 抖音评论爬虫监控脚本

本脚本用于爬取指定批量抖音视频的评论及其回复，将结果保存为CSV文件，并监控增量更新的评论。

原项目：[alphaply/DouyinComments](https://github.com/alphaply/DouyinComments)

本项目在原项目基础上进行了修改，特别感谢alphaply。

## 环境要求

脚本运行需要以下环境：
- asyncio
- httpx
- pandas
- execjs
- nodejs（重要）


## 安装依赖

在运行脚本之前，请确保安装了所有必要的依赖,别忘记安装nodejs：

```bash
pip install httpx pandas PyExecJS
```

## 脚本运行

请先到config.py文件中完成配置，详细配置说明请参考config，然后运行main.py文件：

```bash
python main.py
```

## 输出

- data：包含所有评论及其回复的CSV文件，文件名为视频id_comments/replies.csv。增量更新在相应时间的文件夹中。
- logs：日志文件，包含爬取过程中的信息，按日分割。
