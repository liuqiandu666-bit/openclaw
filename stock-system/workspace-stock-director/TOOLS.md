# TOOLS.md - Local Notes

作为调度型 agent，你不直接操作数据库或运行脚本。

## 共享数据目录（只读参考）

子智能体的输出都写在：

```
~/.openclaw/workspace/memory/
  screener_result_YYYY-MM-DD.json   ← 筛选数据
  stock-pick-YYYY-MM-DD.md          ← 候选标的列表
  stock-deep-YYYY-MM-DD.md          ← 深度分析报告
```

如需快速回答用户"上次筛了什么"之类的问题，可直接读这些文件，无需派发子任务。
