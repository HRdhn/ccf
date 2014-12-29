/******************************************
*    @file   :user_friend.cpp
*    @remark :
*    ����ÿ���û��������ھ�
*
*    ��������
*    for:ÿ���û�
*        for:�����û�
*        ���㹲ͬ�ִ�
*            if:>��ͬ�ִ���>0.2
*                �������ƶ�
*        �ҳ����ƶ���ߵ�10���ھ�
*    д�����ݿ�
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
struct userComNum       //�����û���ÿ���û���ͬ�ķִ���
{
    string userId;
    double sim;
};

struct User             //����ÿ���û���id �ִ� �ھ�
{
    string user_id;
    vector<string> words;
    vector<userComNum> friends;
    map<string, double> word_preference;
};


MYSQL_RES *queryOrder1(MYSQL *mysqlPtr, char *order)        //��msyql��ִ��order������ذ��������ָ��
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

void queryOrder2(MYSQL *mysqlPtr, char *order)              //��mysql��ִ�У������ؽ��������
{
    if (mysql_query(mysqlPtr, order) != 0)
    {
        fprintf(stderr, "query order2 error, %s!\n", mysql_error(mysqlPtr));
        //exit(1);
    }
}

double calc_sim(const User &user1, const User &user2)     //���������û���ͬ�����ƶ�,��ͬ�ִʴﵽ20%�Ĳż��㣬δ�ﵽ�ķ�����Ч��־
{
    set<string> u1(user1.words.begin(), user1.words.end());
    set<string> u2(user2.words.begin(), user2.words.end());
    set<string> intersection;
    set_intersection(u1.begin(), u1.end(), u2.begin(), u2.end(), inserter(intersection, intersection.begin()));
    double usize = intersection.size();
    double u1size = u1.size();
    double common_rate = usize / u1size;
    //cout << intersection.size() << ' ' << common_rate << endl;
    if (common_rate > 0.2)                                  //��ͬ�ִ��Ǳ���a�ķִ����ﵽ20%ʱ�������ƶ�
    {
        double a_aver_pre = 0.0, b_aver_pre = 0.0;          //a��b��ƽ��ƫ��
        for (auto &t : user1.word_preference)       //����a��ƽ��ƫ��
            a_aver_pre += t.second;
        a_aver_pre /= u1.size();
        for (auto &t : user2.word_preference)       //����b��ƽ��ƫ��
            b_aver_pre += t.second;
        b_aver_pre /= u2.size();
        double tem = 0.0;
        double sim = 0.0;
        //cout << "a_avar_pre: " << a_aver_pre << " b_avar_pre: " << b_aver_pre << endl;
        try
        {
            for (auto iter = intersection.begin(); iter != intersection.end(); ++iter)        //���ݹ�ʽ�������ƶ�
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
        return -1000.0;         //������Ч��־
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
	sprintf(getName, "select * from user_name.user_name");      //��user_name�л�ȡuser_id
    MYSQL_RES *userIdRes = queryOrder1(&mysql1, getName);
    MYSQL_ROW userIdRow;

    vector<User> allUser;           //���������û�
    //��������,��user_friends�н������ݱ�
    while ( (userIdRow = mysql_fetch_row(userIdRes) ) != NULL)  //ѭ��ÿ��userid,��ȡ�ִ�
    {

        User curUser;                   //���浱ǰ�û�
        //curUser.words.push_back("����");

        char select[50];
        sprintf(select, "select * from user_words_pre.%s", userIdRow[0]);
        MYSQL_RES *words_res = queryOrder1(&mysql2, select);
        MYSQL_ROW  word_row;
        string tem(userIdRow[0]);
        curUser.user_id = tem;

//        //Ϊÿ���û�����
//        char createTable[100];
//        sprintf(createTable, "create table user_friends.%s(friends varchar(20), similar double default 0.0)", userIdRow[0]);
//        queryOrder2(&mysql3, createTable);

        while ((word_row = mysql_fetch_row(words_res)))                 //��ȡÿ���ִ�
        {
            //cout << word_row[0] << endl;
            curUser.words.push_back(word_row[0]);
            curUser.word_preference[ word_row[0] ] = atof( word_row[3] );
        }

        allUser.push_back(curUser);

        counter++;
    }//while


    cout << "���������ھ�\n";
    //Ѱ�������ھ�  ʱ�临�Ӷ�:O(n2)
    int i = 0;
    for (auto a_iter = allUser.begin(); a_iter != allUser.end(); ++a_iter, i++)        //Ϊÿ���û�aѰ�������ھ�
    {
        if (!(i % 50))                                                  //����
            printf("%d\n", i);
        vector<userComNum> userComNumVec;
        for (auto b_iter = allUser.begin(); b_iter != allUser.end(); ++b_iter)      //��������ÿ���û�b�Ա�
        {
            userComNum b_user;
            if (a_iter != b_iter)
            {
                double similar = calc_sim(*a_iter, *b_iter);        //���������û���ͬ�����ƶ�
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
             [] (const userComNum &u1, const userComNum &u2) {return u1.sim > u2.sim;});    //����a�����ƶȽ�������
        for (auto it = userComNumVec.begin(); it != userComNumVec.end() && it != userComNumVec.begin() + 10; ++it)    //��ǰʮ�����ƶ���ߵ��û�����a��friends��
        {
            a_iter->friends.push_back(*it);
        }
    }//end for

    //д�����ݿ�
    char insert[50];
    cout << "д�����ݿ�\n";
    int j = 0;
    for (auto iter = allUser.begin(); iter != allUser.end(); ++iter)
    {
        if (!(j % 50))                                                  //����
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

