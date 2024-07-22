# 说明
redis4.0之后支持的模块功能。
这里modules下有一个woozset，扩展了zadd方法，自动累加。用法:
zaddtop key member
如果key不存在那么执行后member的score为1
如果key存在那么执行后member的score为之前最高score+1