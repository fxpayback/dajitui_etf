import akshare as ak
import logging
import sys
import time
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etf_test.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

def test_etf_name(symbol):
    """
    测试从AKSHARE获取ETF名称
    :param symbol: ETF代码，如 '510200'
    """
    try:
        logging.info(f"开始获取ETF {symbol} 的信息")
        logging.info(f"AKSHARE版本: {ak.__version__}")
        
        # 测试网络连接
        logging.info("测试网络连接...")
        import requests
        response = requests.get("http://www.baidu.com", timeout=5)
        logging.info(f"网络连接测试结果: {response.status_code}")
        
        # 获取ETF基本信息
        logging.info(f"尝试获取ETF {symbol} 的基金信息...")
        start_time = time.time()
        
        # 方法1：通过基金列表获取
        try:
            fund_list = ak.fund_etf_category_sina()
            logging.info(f"成功获取ETF基金列表，共 {len(fund_list)} 条记录")
            
            # 查找指定ETF
            etf_info = fund_list[fund_list['基金代码'] == symbol]
            if not etf_info.empty:
                logging.info(f"方法1 - 在基金列表中找到ETF: {etf_info.iloc[0]['基金名称']}")
            else:
                logging.info(f"方法1 - 未在基金列表中找到ETF {symbol}")
        except Exception as e:
            logging.error(f"方法1出错: {str(e)}", exc_info=True)
        
        # 方法2：通过ETF实时行情获取
        try:
            etf_data = ak.fund_etf_spot_em()
            logging.info(f"成功获取ETF实时行情，共 {len(etf_data)} 条记录")
            
            # 查找指定ETF
            etf_info = etf_data[etf_data['代码'] == symbol]
            if not etf_info.empty:
                logging.info(f"方法2 - 在实时行情中找到ETF: {etf_info.iloc[0]['名称']}")
            else:
                logging.info(f"方法2 - 未在实时行情中找到ETF {symbol}")
        except Exception as e:
            logging.error(f"方法2出错: {str(e)}", exc_info=True)
        
        # 方法3：通过历史行情获取
        try:
            hist_data = ak.fund_etf_hist_sina(symbol)
            if not hist_data.empty:
                logging.info(f"方法3 - 成功获取ETF {symbol} 的历史数据")
            else:
                logging.info(f"方法3 - 未获取到ETF {symbol} 的历史数据")
        except Exception as e:
            logging.error(f"方法3出错: {str(e)}", exc_info=True)
        
        end_time = time.time()
        logging.info(f"获取ETF信息总耗时: {end_time - start_time:.2f} 秒")
        
    except Exception as e:
        logging.error(f"获取ETF信息时发生错误: {str(e)}", exc_info=True)
    
    logging.info("测试完成")
    logging.info("-" * 50)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        symbol = sys.argv[1]
    else:
        symbol = "510200"  # 默认测试中小板ETF
    
    logging.info(f"开始测试 - {datetime.now()}")
    logging.info(f"Python版本: {sys.version}")
    logging.info(f"运行平台: {sys.platform}")
    
    test_etf_name(symbol) 