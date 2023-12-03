'''
對FoodPanda餐點資料做清整，詳見前一層README
Cleaning up FoodPanda menu data, refer to the previous README for details.
'''

import pandas as pd
import os
from collections import Counter

for i in range(1, 12):
    # 創建清整資料夾
    post_save_dir = './clean_Data'
    if not os.path.exists(post_save_dir):
        print(f"目錄 {post_save_dir} 不存在，已創建 {post_save_dir}")
        os.makedirs(post_save_dir)
    else:
        print(f"{post_save_dir} 目錄已存在，將開始清整")

    # 讀取 CSV 檔案到 DataFrame
    df = pd.read_csv('./row_Data/dishes_info_{i}.csv',
                     header=None, encoding='utf-8-sig')

    def price():
        global df
        # 刪除菜價小於10元的資料
        df = df[df.iloc[:, 4] >= 10]

    def duplicates():
        global df
        # 刪除擁有相同菜名id的整欄
        df = df.drop_duplicates(subset=2, keep='first')

    def remove_none():
        global df
        # 刪除第9列為空的整欄
        df = df.dropna(subset=[8])

    def loc():
        global df
        # 使用 str.replace 將地址中的奇怪字符替換為空字符串
        df.iloc[:, 5] = df.iloc[:, 5].str.replace(
            '^\(△\) |\(△\)|\(O\) |\(O\)|\(X\) |\(X\)|\(○\) |\(○\)|\(x\) \(x\)|\(△|\( △\)|  \(M\) |\(M\) |\(#\) |\(#\)', '', regex=True)

    def remove_undefinable_items():
        global df
        # 要刪除的品項列表
        items_to_remove = ['Soups', 'Noodles', 'Hot Pot',
                           'Fried Chicken', 'Bento', 'Teppanyaki']

        # 使用 str.replace 將指定品項替換為空字符串
        for item in items_to_remove:
            df.iloc[:, 8] = df.iloc[:, 8].str.replace(item, '', regex=False)

    def merge_columns():
        global df
        # 檢查是否有 NaN 值，如果有，填充為空字符串
        df.iloc[:, 8] = df.iloc[:, 8].fillna('NAN')
        # 根據先決條件進行品項的整合
        conditions = [
            ('Brunch', ['Breakfast', 'Sandwiches & Toast']),
            ('Drinks', ['Drinks', 'Coffee']),
            ('Desserts', ['Desserts', 'Dou Hua', 'Donut', 'Cakes']),
            ('Vegetarian', ['Vegetarian']),
            ('American', ['American', 'Pizza', 'Steak', 'Burgers']),
            ('J&K', ['Japanese', 'Korean']),
            ('International', ['International', 'Southeast Asian', 'Curry']),
            ('TW', ['Taiwanese', 'Lu wei', 'Snacks', 'TW Fried Chicken',
                    'Fried Rice', 'Chinese', 'Hong Kong', 'Dumpling', 'Congee', 'Healthy'])
        ]

        for result, conditions_list in conditions:
            df.iloc[:, 8] = df.apply(lambda row: result if any(
                cond in row[8] for cond in conditions_list) else row[8], axis=1)

    # def find_unique_items():
    #     global df
    #     # 將 '品項' 欄位中的品項分割成列表
    #     items_list = df.iloc[:, 8].str.split(',')

    #     # 找出曾經單獨出現在某個欄位中的品項
    #     solo_items = set()
    #     for items in items_list.dropna():
    #         if len(items) == 1:
    #             solo_items.add(items[0])

    #     # 列印結果
    #     print("曾經單獨出現在某個欄位中的品項:")
    #     for item in solo_items:
    #         print(item)

    # def count_items():
    #     global df
    #     # 將 '品項' 欄位中的品項分割成列表
    #     items_list = df.iloc[:, 8].str.split(',')

    #     # 展平列表
    #     flattened_items = [item for sublist in items_list.dropna()
    #                        for item in sublist]

    #     # 使用 Counter 計算每個品項的數量
    #     items_counter = Counter(flattened_items)

    #     # 將結果寫入 txt 檔案
    #     with open('item_counts_1.txt', 'w', encoding='utf-8') as file:
    #         file.write("各品項數量:\n")
    #         for item, count in items_counter.items():
    #             file.write(f"{item}: {count} 項。\n")

    if __name__ == "__main__":
        price()
        duplicates()
        loc()
        remove_none()
        remove_undefinable_items()
        merge_columns()
        # find_unique_items()
        # count_items()

    # 將清理後的資料寫回CSV檔案
    df.to_csv(f'./clean_Data/cleaned_data_FoodPanda_{i}.csv', index=False,
              header=False, encoding='utf-8-sig')

    print("已順利清整結束")
