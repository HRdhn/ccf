该仓库保存了本次新闻推荐的主要代码。

calc_user_pre.cpp:
处理前期数据，计算每个用户对分词的偏好，保存到user_word_pre.sql中。

user_add_news_pre.cpp:
处理前期数据，计算每个用户对该条新闻的偏好，保存在test20.sql中。

user_friend.cpp:
根据用户对分词的偏好，为每个用户寻找十个相似邻居，保存到user_friends.sql


new_recommendation_accor_friends.cpp:
从每个用户邻居看过，而该用户没看过的新闻中，选择性的推荐给当前用户，保存到
user_rec_news.sql

examine_result.cpp:
检验推荐效果，将用于参考的新闻与推荐出的五条新闻对比，若五条新闻中有一条与该参考新闻匹配，算成功一次。参考新闻保存在reference.sql中。