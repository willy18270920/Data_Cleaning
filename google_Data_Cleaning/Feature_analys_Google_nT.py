'''
對Google店家評論做特徵工程
Performing feature engineering on Google business reviews.
'''
import pandas as pd
import os

# 創建清整資料夾
post_save_dir = './feature_analys'
if not os.path.exists(post_save_dir):
    print(f"目錄 {post_save_dir} 不存在，已創建 {post_save_dir}")
    os.makedirs(post_save_dir)
else:
    print(f"{post_save_dir} 目錄已存在，將開始進行特徵工程")


def Feature_analys():
    # 創建一個新的DataFrame來儲存結果
    result_df = pd.DataFrame(columns=['StoreID', 'StoreName', 'Label'])

    # 定義詞彙分類
    categories = {
        '美味': ['好吃', '吃', '味道', '口味', '好喝', '美味', '口感', '湯頭', '新鮮'],
        '服務': ['親切', '份量', '態度', '服務態度', '店員', '時間', '飽', '兒童', '營業', '禮貌', '快'],
        '環境': ['不錯', '乾淨', '店家', '整體', '裝潢', '網美', '包廂', '懷舊', '環境', '設計', '舒適', '寬敞', '氣氛', '空間', '佈置', '風格', '內部', '外觀'],
    }

    # 創建一個新的欄位 'Label' 來儲存分類結果
    merged_comments['Label'] = None

    # 遍歷每一行資料
    for index, row in merged_comments.iterrows():
        # 將所有評論合併為一個長字串
        all_comments = ' '.join(str(row[2]).split())  # 第三欄是評論

        # 初始化分類標籤
        label = None

        # 檢查每個詞彙是否存在於評論中，並分類
        for category, keywords in categories.items():
            if any(keyword in all_comments for keyword in keywords):
                label = category
                break  # 如果找到相符的詞，就不用再檢查其他分類

        # 將分類結果存入 'Label' 欄位
        merged_comments.at[index, 'Label'] = label

        # 對該分組進行最終標籤的判斷
        if label is not None:
            # 根據你的邏輯決定最終標籤
            # 這部分與你提供的程式碼相似，可根據需要進行修改
            taste_count = len(
                set(all_comments.split()).intersection(categories['美味']))*0.75
            service_count = len(
                set(all_comments.split()).intersection(categories['服務']))*0.9
            environment_count = len(
                set(all_comments.split()).intersection(categories['環境']))

            # 決定最終標籤
            if taste_count >= max(service_count, environment_count):
                label = '美味'
            else:
                # 選擇其他三個類別中數量最多的
                label = max(
                    ('服務', service_count),
                    ('環境', environment_count),
                    key=lambda x: x[1]
                )[0]

            # 添加結果到新的 DataFrame
            result_df = pd.concat([result_df, pd.DataFrame({
                'StoreID': [row[0]],
                'StoreName': [row[1]],
                'Label': [label]
            })], ignore_index=True)

    # 將處理後的數據存儲回原始檔案
    output_file_path = f"./feature_analys/Feature_analys_{i}.csv"
    result_df.to_csv(output_file_path, index=False, encoding='utf-8-sig')
    print(f"Feature_analys_{i} 已處理完畢")


for i in range(1, 12):
    # 讀取處理後的數據
    df = pd.read_csv(f'./clean_Data/cleaned_data_Google_{i}.csv',
                     header=None, encoding='utf-8-sig')

    df[2] = df[2].astype(str)
    # 合併相同店家ID的評論
    merged_comments = df.groupby(0).agg(
        {1: 'first', 2: ' '.join}).reset_index()

    if __name__ == "__main__":
        Feature_analys()


# # 使用Counter計算詞頻
# word_count = Counter(filtered_words)

# # 取得前40個最常出現的詞
# top_40_words = word_count.most_common(40)

# # 印出結果
# for word, count in top_40_words:
#     print(f'{word}: {count}次')
