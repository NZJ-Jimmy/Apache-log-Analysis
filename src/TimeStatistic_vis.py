import matplotlib.pyplot as plt

# 输入文件
with open('output/TimeStatistic.txt', 'r') as file:
    data = file.read()

# 数据处理
categories = {"Day": [], "Hour": [], "Month": [], "Week Day": []}
for line in data.strip().split("\n"):   # 去除首尾空格，按行分割
    key, value = line.split("\t")       # 按制表符分割键值
    category, label = key.split(":")    # 按冒号分割键类别和标签
    value = int(value)                  # 将值转换为整数
    categories[category].append((label, value)) # 添加到对应类别

# 绘制各类条形图
for category, data in categories.items():
    labels, values = zip(*data) # 解压键值对
    plt.figure()    # 创建新图
    plt.bar(labels, values) # 绘制条形图
    plt.title(f"{category} Distribution")   # 设置标题
    plt.xticks(rotation=45 if len(labels) > 10 else 0)  # 若标签太多，设置标签旋转角度

# 显示图像
plt.show()