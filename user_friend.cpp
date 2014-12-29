/******************************************
*    @file   :user_friend.cpp
*    @remark :
*    计算每个用户的相似邻居
*
*    读入数据
*    for:每个用户
*        for:其余用户
*        计算共同分词
*            if:>共同分词数>0.2
*                计算相似度
*        找出相似度最高的10个邻居
*    写入数据库
*
*    @date   :2014/12/20
*    @author :HaoNan Dai
******************************************/

#include<winsock2.h>
#include"mysql.h"
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<iostream>
#include<sstream>
#include<algorithm>
#include<vector>
#include<string>
#include<set>
#include<map>
#include<stdexcept>

using namespace std;
struct userComNum       //保存用户和每个用户共同的分词数
{
    string userId;
    double sim;
};

struct User             //保存每个用户的id 分词 邻居
{
    string user_id;
    vector<string> words;
    vector<userComNum> friends;
    map<string, double> word_preference;
};


MYSQL_RES *queryOrder1(MYSQL *mysqlPtr, char *order)        //在msyql上执行order命令，返回包含结果的指针
{
    MYSQL_RES *result = nullptr;
    if (mysql_query(mysqlPtr, order) != 0)
    {
        fprintf(stderr, "query order1 error, %s!\n", mysql_error(mysqlPtr));
        //exit(1);
    }
    else if ( (result = mysql_store_result(mysqlPtr)) == NULL )
    {
        fprintf(stderr, "save result error!\n");
        //exit(1);
    }
    else
        return result;
}

void queryOrder2(MYSQL *mysqlPtr, char *order)              //在mysql上执行，不返回结果的命令
{
    if (mysql_query(mysqlPtr, order) != 0)
    {
        fprintf(stderr, "query order2 error, %s!\n", mysql_error(mysqlPtr));
        //exit(1);
    }
}

double calc_sim(const User &user1, const User &user2)     //计算两个用户共同的相似度,共同分词达到20%的才计算，未达到的返回无效标志
{
    set<string> u1(user1.words.begin(), user1.words.end());
    set<string> u2(user2.words.begin(), user2.words.end());
    set<string> intersection;
    set_intersection(u1.begin(), u1.end(), u2.begin(), u2.end(), inserter(intersection, intersection.begin()));
    double usize = intersection.size();
    double u1size = u1.size();
    double common_rate = usize / u1size;
    //cout << intersection.size() << ' ' << common_rate << endl;
    if (common_rate > 0.2)                                  //共同分词是比上a的分词数达到20%时计数相似度
    {
        double a_aver_pre = 0.0, b_aver_pre = 0.0;          //a、b的平均偏好
        for (auto &t : user1.word_preference)       //计算a的平均偏好
            a_aver_pre += t.second;
        a_aver_pre /= u1.size();
        for (auto &t : user2.word_preference)       //计算b的平均偏好
            b_aver_pre += t.second;
        b_aver_pre /= u2.size();
        double tem = 0.0;
        double sim = 0.0;
        //cout << "a_avar_pre: " << a_aver_pre << " b_avar_pre: " << b_aver_pre << endl;
        try
        {
            for (auto iter = intersection.begin(); iter != intersection.end(); ++iter)        //根据公式计数相似度
                tem += (user1.word_preference.at(*iter) - a_aver_pre) * (user2.word_preference.at(*iter) - b_aver_pre);
            //cout << "tem : " << tem << endl;
        }
        catch (out_of_range err)
        {
            err.what();
            tem = 0.0;
        }
        sim = tem * common_rate;
        return sim;
    }
    else
        return -1000.0;         //返回无效标志
}
void connector(char *sqlName, MYSQL *mysql)
{
	if (!mysql_real_connect(mysql, "localhost", "root", "dhn15158882311", sqlName, 3306, NULL, 0))	//connect to test2011
	{
      fprintf(stderr,"%s\n",mysql_error(mysql));											//prototype   const char *mysql_error(MYSQL *mysql)
      exit(1);
	}
	else
	{
		fprintf(stderr, "mysql connect to user_name successfully!\n");
	}
}

void setName(MYSQL *mysql)
{
	if (mysql_query(mysql, "set names gbk"))												//change character set
	{
		fprintf(stderr, "error!,%s\n", mysql_error(mysql));
		exit(1);
	}
}
void setCharacter(MYSQL *mysql)
{
	if ( mysql_set_character_set( mysql , "gbk" ))		//change the character set
	{
		fprintf(stderr, "error,%s\n", mysql_error(mysql));
	}
}

int main()
{
	MYSQL mysql1;
	MYSQL mysql2;
    MYSQL mysql3;
	char sqlName1[20] = "user_name";
	char sqlName2[20] = "user_words_pre";
	char sqlName3[20] = "user_friends";
	mysql_init(&mysql1);	//initialize
	mysql_init(&mysql2);
	mysql_init(&mysql3);


    connector(sqlName1, &mysql1);
    connector(sqlName2, &mysql2);
    connector(sqlName3, &mysql3);

    setName(&mysql1);
    setName(&mysql2);
    setName(&mysql3);

    setCharacter(&mysql1);
	setCharacter(&mysql2);
	setCharacter(&mysql3);


	int counter = 0;
	char getName[20];
	sprintf(getName, "select * from user_name.user_name");      //从user_name中获取user_id
    MYSQL_RES *userIdRes = queryOrder1(&mysql1, getName);
    MYSQL_ROW userIdRow;

    vector<User> allUser;           //保存所有用户
    //读入数据,在user_friends中建立数据表
    while ( (userIdRow = mysql_fetch_row(userIdRes) ) != NULL)  //循环每个userid,读取分词
    {

        User curUser;                   //保存当前用户
        //curUser.words.push_back("湖南");

        char select[50];
        sprintf(select, "select * from user_words_pre.%s", userIdRow[0]);
        MYSQL_RES *words_res = queryOrder1(&mysql2, select);
        MYSQL_ROW  word_row;
        string tem(userIdRow[0]);
        curUser.user_id = tem;

//        //为每个用户建表
//        char createTable[100];
//        sprintf(createTable, "create table user_friends.%s(friends varchar(20), similar double default 0.0)", userIdRow[0]);
//        queryOrder2(&mysql3, createTable);

        while ((word_row = mysql_fetch_row(words_res)))                 //读取每个分词
        {
            //cout << word_row[0] << endl;
            curUser.words.push_back(word_row[0]);
            curUser.word_preference[ word_row[0] ] = atof( word_row[3] );
        }

        allUser.push_back(curUser);

        counter++;
    }//while


    cout << "计算相似邻居\n";
    //寻找相似邻居  时间复杂度:O(n2)
    int i = 0;
    for (auto a_iter = allUser.begin(); a_iter != allUser.end(); ++a_iter, i++)        //为每个用户a寻找相似邻居
    {
        if (!(i % 50))                                                  //计数
            printf("%d\n", i);
        vector<userComNum> userComNumVec;
        for (auto b_iter = allUser.begin(); b_iter != allUser.end(); ++b_iter)      //和其他的每个用户b对比
        {
            userComNum b_user;
            if (a_iter != b_iter)
            {
                double similar = calc_sim(*a_iter, *b_iter);        //计算两个用户共同的相似度
                b_user.userId = b_iter->user_id;
                b_user.sim = similar;
//                printf("%s-%s:%f sim\n", (a_iter->user_id).c_str(), (b_user.userId).c_str(), b_user.sim);
//                system("pause");

            }
            if (b_user.sim != -1000.0)
            {
                userComNumVec.push_back(b_user);
            }
        }

        sort(userComNumVec.begin(), userComNumVec.end(),
             [] (const userComNum &u1, const userComNum &u2) {return u1.sim > u2.sim;});    //按与a的相似度进行排序
        for (auto it = userComNumVec.begin(); it != userComNumVec.end() && it != userComNumVec.begin() + 10; ++it)    //将前十个相似度最高的用户插入a的friends中
        {
            a_iter->friends.push_back(*it);
        }
    }//end for

    //写入数据库
    char insert[50];
    cout << "写入数据库\n";
    int j = 0;
    for (auto iter = allUser.begin(); iter != allUser.end(); ++iter)
    {
        if (!(j % 50))                                                  //计数
            printf("%d\n", j);
        j++;
        for (auto it = iter->friends.begin(); it != iter->friends.end(); ++it)
        {
            sprintf(insert, "insert into user_friends.%s values (%s, %f)", (iter->user_id).c_str(), (*it).userId.c_str(), (*it).sim);
            queryOrder2(&mysql3, insert);
        }
    }//end for


    cout << counter << endl;

	mysql_close(&mysql1);
	mysql_close(&mysql2);

    return 0;

}

