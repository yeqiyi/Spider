import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import selenium.webdriver.support.ui as ui
def save_to_mysql(df_data,table):
    connect=create_engine('mysql+pymysql://root:123456@localhost:3306/COVID?charset=utf8')
    if table == "data":
        df_data.to_sql(name=table,con=connect,index=False,if_exists='append')
    else:
        df_data.to_sql(name=table, con=connect, index=False, if_exists='replace')

def resolve_info(data, tag):
    """
    解析数据
    @param data:
    @param tag:
    @return:
    """
    if tag == 'province':
        # 城市
        data_name =  [string for string in data.find('p', class_='subBlock1___3cWXy').strings][0]
        return [data_name]
    else:
        # 省份
        data_name = [string for string in data.find('p', class_='subBlock1___3cWXy').strings][0]
            # 累计确诊人数
        data_sum_diagnose = data.find('p', class_='subBlock3___3dTLM').string
            # 死亡人数
        data_death = data.find('p', class_='subBlock4___3SAto').string

            # 治愈人数
        data_cure = data.find('p', class_='subBlock5___33XVW').string
        data_time = datetime.now() + timedelta(-1)
        data_time_str = data_time.strftime('%Y-%m-%d')
        data_cure=int(data_cure.replace(',',''))
        data_sum_diagnose=int(data_sum_diagnose.replace(',','')) #数据去除千分符
        data_death=int(data_death.replace(',',''))
        data_doubt=None
        return [data_name, data_cure,data_time_str, data_death,data_doubt,data_sum_diagnose]
    # 设置昨天的日期作为数据日期

if __name__ == '__main__':
        
        print("capturing data……")
        url = 'https://ncov.dxy.cn/ncovh5/view/pneumonia'
        executable_path = "D:\chromedriver\chromedriver.exe"
            # 设置不弹窗显示
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-gpu')
        browser = webdriver.Chrome(options=chrome_options, executable_path=executable_path)
            # 弹窗显示
            # browser = webdriver.Chrome(executable_path=executable_path)
        wait = ui.WebDriverWait(browser, 10)
        browser.get(url)
        for x in browser.find_elements_by_class_name('expandRow___1Y0WD'):
            link = webdriver.ActionChains(browser).move_to_element(x).click(x).perform()
          # 输出网页源码
        content = browser.page_source
        soup = BeautifulSoup(content, 'html.parser')
        soup_province_class = soup.find('div', class_='areaBox___Sl7gp themeA___1BO7o numFormat___nZ7U7 flexLayout___1pYge').find_all('div', class_='areaBlock1___3qjL7')

        list_province_data = []
        list_province=[]
        for per_province in soup_province_class:
                # 如果存在img标签，则认为该数据有效，进行解析。（否则该数据应该是表头，应跳过）
                if per_province.find_all('p')[0].find('img'):
                        # 省份数据
                        list_current_province = resolve_info(per_province, 'data')
                        province=resolve_info(per_province,'province')
                        list_province_data.append(list_current_province)
                        list_province.append(province)
        # 数据转换成 DataFrame
        df_province_data = pd.DataFrame(list_province_data,
                                        columns=['provinceName', 'cure', 'date', 'dead', 'doubt','infect'])
        df_province=pd.DataFrame(list_province,columns=['provinceName'])
        browser.quit()
        print("capture completed")
        print("inserting data into mysql……")

        save_to_mysql(df_province_data,'data')
        print("insert completed!")


