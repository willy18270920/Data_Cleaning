'''
對FoodPanda餐點資料做清整，詳見前一層README
Cleaning up FoodPanda menu data, refer to the previous README for details.
'''
import pandas as pd
import re
import jieba
from jieba import analyse
from collections import Counter
import os

# 創建清整資料夾
post_save_dir = './clean_Data'
if not os.path.exists(post_save_dir):
    print(f"目錄 {post_save_dir} 不存在，已創建 {post_save_dir}")
    os.makedirs(post_save_dir)
else:
    print(f"{post_save_dir} 目錄已存在，將開始清整")

for i in range(1, 12):
    # 載入停用詞表
    stopwords_path = 'cn_stopwords.txt'
    jieba.analyse.set_stop_words(stopwords_path)  # 替換為你的停用詞表路徑

    # 讀取停用詞表的內容
    with open(stopwords_path, 'r', encoding='utf-8') as f:
        stopwords_content = f.read()

    # 將停用詞轉為集合
    stopwords = set(stopwords_content.splitlines())

    def process_text(text):
        # 定義文字處理的函數
        # 去除HTML標籤
        text = re.sub(r'<.*?>', '', str(text))
        # 去除標點符號和特殊符號
        text = re.sub(r'[^\w\s]', '', text)
        # 將所有文字轉為小寫
        text = text.lower()
        return text

    def chinese_word_segmentation(text):
        # 斷句結疤處理
        words = jieba.cut(text, cut_all=False)
        result = ' '.join(words)
        return result

    def remove_stopwords(text):
        # 去除停用詞
        words = [word for word in text.split() if word not in stopwords]
        result = ' '.join(words)
        return result

    def preprocess_and_segment(file_path, output_file_path):
        # 步驟1: 文字處理
        df = pd.read_csv(file_path, header=None, encoding='utf-8-sig')
        df.iloc[:, 2] = df.iloc[:, 2].apply(process_text)

        # 步驟2: 斷詞
        df.iloc[:, 2] = df.iloc[:, 2].apply(chinese_word_segmentation)

        # 步驟3: 停用詞去除
        df.iloc[:, 2] = df.iloc[:, 2].apply(remove_stopwords)

        # 將處理後的數據存儲回原始檔案
        df.to_csv(output_file_path, index=False,
                  header=None, encoding='utf-8-sig')
        print(f"處理後的數據已存儲到 {output_file_path}")

    if __name__ == "__main__":
        preprocess_and_segment(f'./row_Data/Google_{i}.csv',
                               f'./clean_Data/cleaned_data_Google_{i}.csv')

# # 讀取處理後的數據
# df = pd.read_csv('./clean_Data/cleaned_data_Google.csv',
#                  header=None, encoding='utf-8-sig')

# # 將第3列的所有詞組合成一個長字串
# all_words = ' '.join(df.iloc[:, 2].fillna('').values)

# # 使用jieba.cut進行中文斷詞
# seg_list = jieba.cut(all_words)

# # 去除停用詞
# filtered_words = [word for word in seg_list if word not in stopwords]

# # 使用Counter計算詞頻
# word_count = Counter(filtered_words)

# # 取得前40個最常出現的詞
# top_40_words = word_count.most_common(40)

# # 印出結果
# for word, count in top_40_words:
#     print(f'{word}: {count}次')
