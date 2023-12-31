# 宏观量化投资

星球专属文档与代码库。

## 相关主题

### 因子投资
本星球的一大主题就是量化多因子投资框架，主要围绕着MSCI因子、风险模型、指数构建策略展开。

相关文档：
- [Barra框架简介](https://github.com/robustness-data/quant-universe/blob/master/docs/factor%20investing/barra_intro.md)

### 宏观研究与其在量化投资里的应用
介绍宏观经济对于资产价格的影响，并搜集实用的利用宏观信号进行量化投资的例子。

## Q&A：怎么运行Streamlit数据可视化App？

### 第一步： 准备相关python环境
推荐python3.8或以上的版本，必须安装的库
- pathlib
- pandas
- streamlit
- plotly
- plotly.express
- openpyxl

如果发现有遗漏的库也请告诉我或在Issue里告知。

### 第二步：运行Streamlit App
打开terminal，移至 quant-universe/src/app 文件夹，然后运行
```commandline
streamlit run home.py
```
之后你的浏览器就会弹出一个窗口，该窗口就是Streamlit App，可以通过左侧的边栏选择你要看的数据。

更多关于Steamlit的使用方法请参见官网：https://docs.streamlit.io/

