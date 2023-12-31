import streamlit as st

tab_1, tab_2 = st.tabs(['量化因子模型的种类','Barra因子模型'])


with tab_1:
    st.markdown(
        body="""
        ### 量化因子模型的种类
        
        在介绍Barra因子模型前，我们要有一个概念就是，Barra因子模型是所有量化因子模型里的其中一种。
        
        广义的因子模型旨在从高纬度的数据里提取出低纬度的主要参考因素，使得这些主要因素可以解释大部分的资产收益率。而具体到量化投资领域，目前常见的因子模型有几种类型：
        - 统计因子模型，例如PCA模型
        - 宏观因子模型，即APT模型 
        - Barra 多因子模型
        - Fama-French 多因子模型
        
        在以上模型里，统计因子模型主要基于回报率的统计性质来提取出最大程度解释跨资产收益率的几大因素，不加入研究者的主观判断以及外部数据，因此也更容易受到数据中噪音的影响，特别是从几千只股票中提取低纬度因子时。
        
        基于此，目前最主流的多因子模型都是基于一定的金融学理论来选择因子，并通过统计手段来调整参数以达到最好的效果。然而，后三种基于金融学理论的多因子模型仍然有很大的差异。
        
        宏观因子模型的特点是，其因子是可以被观测到的宏观变量的时间序列。搭建该模型时，研究者需要估计的是资产对于这些宏观变量的风险暴露（factor loadings/exposures），即在某一个观察区间内，当通胀上升1%以后，英伟达股票的回报率平均会变多少。
        
        而Barra多因子模型则是反过来。研究员们先通过已有的金融学研究来筛选可以用来解释股票回报率的公司指标或市场表现，然后对样本内的每一个公司都建立一个有多纬度的指标向量，形成一个主观的风险暴露值的分布。之后，研究员会通过统计回归手段在挖掘出一套可以最大程度解释所有样本内资产回报率的因子回报率（factor returns）。用通俗点的话讲就是："高增长预期或低估值的股票需要分别得到什么样的回报率，才可以同时解释英伟达和一种小盘股在过去一周的回报率？"
        
        学界用了几十年的Fama-French模型，则更粗糙一点："如果我买那些P/E最低的前10%的股票，卖出P/E最高的前10%的股票，我会赚多少？"该模型的两大特点是，1）因为构建这种排序投资组合需要在每个格子里都保持一定量的股票，根据简单的curse-of-dimensionality原理，无论是用的格子有多么粗糙，美股里的3000+只流动性还不错的股票只能容纳若干个纬度的因子，2015年的FF5就是一个很好的例子；2）该方法本身不具有解释个股回报的能力，例如将构建出来的因子组合当作宏观变量纳入APT模型里才可以对单股票进行多因子分析。
    """
    )

with tab_2:
    st.markdown(
        body="""
        
        ### Barra 模型的历史
        
        根据MSCI官网描述，Stephen Ross 套利定价模型出现的前一年，1975年，Barra发布了第一代股票多因子模型。1987年，Barra发布了第一代固收多因子模型。两年之后，1989年，第一代覆盖全球股票市场的多因子模型Global Equity Model（GEM）问世。到了1994年，摩根大通开发出了RiskMetrics资产定价模型，后被MSCI于2010年收购，成为了现在MSCI Barra风控模型，尤其是固收模型的护城河之一。再后来，Barra在2004年被MSCI收购，也正式开启了MSCI整合定价、风控、指数策略整套量化投资流程的阶段。
        
        以上虽然听起来像是一个MSCI的宣传文，但是它也恰恰是过去四十年整个量化行业发展的一个缩影。
        
        在MSCI的量化工具体系里，Barra的因子模型，或者业内多称为风险模型，算所谓的"中台"工具。因子模型主要被用来分析投资组合的风险来源，即可帮助前台设计策略，也可以给后台的风控部门提供投资策略的风险指标。
        
        MSCI的老本行，指数设计，则是一个更偏"前台"的工作。其基本内容就是依据各种投资策略，比如因子、主题、行业策略，结合股票筛选与投资组合优化，构建一系列用于参考的投资组合。这些指数后来就可以被各大基金或者ETF用来复制形成可投资的组合，或者被主动型基金用来当作参考物计算超额收益。
        
        ### Barra 因子模型的结构
        
        Barra 多因子模型认为，股票的价格变动，是由若干个系统性风险因子，加上具有特异性的个股回报率来决定的。
        
        系统性风险反映的是依据基本面和经济学原理形成的定价逻辑，与经济周期密切相关，通常没有办法被分散。而系统性风险的因子回报率，大多数时候也反映了承担该风险所要求的回报，即风险溢价。这也是为什么很多时候你会看到业内把常见的风格因子模型说成是风险模型， 把这些因子称为 $\\beta$。系统性因子所带来的回报率其实是需要暴露在相关风险中才能获得的，即“高风险高回报”。
        
        当然，Barra模型里的因子，不都是风险溢价的表现，也包括其他的系统性驱动力，例如市场情绪、供需关系、拥挤度，等等。这里面有些因子反映的是由于市场摩擦而产生的短期效应，与经济周期没有强相关性，投资容量也较小。当某一种市场特点消失后，依据该特点产生的因子也可能随之消失。因此，虽然从数学上来看，这些因子是“共同因子”，但严格意义上讲不属于系统性因子，而更像是 $\\alpha$。
        
        数学上，我们可以这么定义Barra多因子模型：
        
        $$
        r_{i}(t) = \sum_{k=1}^K X_{i,k}(t) f_k(t) + \epsilon_i(t) 
        $$
        
        - $r_i(t)$ : 资产 $i$ 在 $t$ 时间点相对短期利率的超额回报率。
        - $X_{i,k}(t)$ : 资产 $i$ 在 $t$ 时间点对于因子 $k$ 的风险暴露，需要使用 $t$ 时间点之前的数据构成。
        - $f_k(t)$ : 因子 $k$ 在 $t$ 时间点的因子回报率。
        - $\epsilon_i(t)$ : 资产 $i$ 在 $t$ 时间点的剔除掉系统性因素的特异性回报率。
        
        由于该因子模型不涉及时间序列建模，即所有变量都在同一期，为了简洁，我们在下面的公式里会省略时间项。
        
        在Barra因子模型的术语里，$X_{i,k} f_k$ 被称为common factor，姑且翻译为“共同因子”或“系统性因子”，反映的是系统性风险溢价；而$\epsilon_i$ 被称为 specific factor，反映的是公司特异性的，没有被系统性因子解释的回报率。
        
        需要注意的是，由于 Barra 因子模型属于通过金融学原理发掘系统性因子的模型，其拟合股票回报率的能力，取决于研究者挖掘有效因子的能力。换句话说，$\epsilon_i$ 中几乎一定存在没有被发掘的因子。而找出这些未被发掘的因子，且根据该因子进行投资，是那些“聪明投资者”获利的来源，即我们常说的 $\\alpha$。
        
        而我们也可以用该观点来理解因子模型的发展史。多因子模型自CAPM以来的发展，其实就是将粗糙的 $\\beta$ 用金融学研究一步步细化的过程。而每一次的细化，都代表着投资者发现了新的可以解释资产回报率的因素，并且，在该因素逐渐被人所熟知后，变成了新的 $\\beta$，以此往复。
        
        """
    )
