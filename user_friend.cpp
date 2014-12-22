/******************************************
*    ��������                              *
*    for:ÿ���û�                          *
*        for:�����û�                      *
*        ���㹲ͬ�ִ�                       *
*            if:>��ͬ�ִ���>0.2            *
*                �������ƶ�                *
*        �ҳ����ƶ���ߵ�10���ھ�           *
*    д�����ݿ�                            *
                                          *
*    ʱ�临�Ӷȣ�O(n2)                     *
*    date:2014/12/20                      *
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

using namespace std;

struct User             //����ÿ���û���id �ִ� �ھ�
{
    string user_id;
    vector<string> words;
    vector<string> friends;
};

struct userComNum       //�����û���ÿ���û���ͬ�ķִ���
{
    string userId;
    int commonNum;
};



MYSQL_RES *queryOrder1(MYSQL *mysqlPtr, char *order)        //��msyql��ִ��order������ذ��������ָ��
{
    MYSQL_RES *result = nullptr;
    if (mysql_query(mysqlPtr, order) != 0)
    {
        fprintf(stderr, "query order1 error, %s!\n", mysql_error(mysqlPtr));
        exit(1);
    }
    else if ( (result = mysql_store_result(mysqlPtr)) == NULL )
    {
        fprintf(stderr, "save result error!\n");
        exit(1);
    }
    else
        return result;
}

void queryOrder2(MYSQL *mysqlPtr, char *order)              //��mysql��ִ�У������ؽ��������
{
    if (mysql_query(mysqlPtr, order) != 0)
    {
        fprintf(stderr, "query order2 error, %s!\n", mysql_error(mysqlPtr));
        exit(1);
    }
}

int calc_common_words(const User &user1, const User &user2)     //���������û���ͬ�ķִ���
{
    set<string> u1(user1.words.begin(), user1.words.end());
    set<string> u2(user2.words.begin(), user2.words.end());
    set<string> intersection;
    set_intersection(u1.begin(), u1.end(), u2.begin(), u2.end(), inserter(intersection, intersection.begin()));
    return intersection.size();
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


	if (!mysql_real_connect(&mysql1, "localhost", "root", "dhn15158882311", sqlName1, 3306, NULL, 0))	//connect to test2011
	{
      fprintf(stderr,"%s\n",mysql_error(&mysql1));											//prototype   const char *mysql_error(MYSQL *mysql)
      exit(1);
	}
	else
	{
		fprintf(stderr, "mysql1 connect to user_name successfully!\n");
	}
	if (!mysql_real_connect(&mysql2, "localhost", "root", "dhn15158882311", sqlName2, 3306, NULL, 0))	//connect to test2011
	{
      fprintf(stderr,"%s\n",mysql_error(&mysql2));											//prototype   const char *mysql_error(MYSQL *mysql)
      exit(1);
	}
	else
	{
		fprintf(stderr, "mysql2 connect to test20 successfully!\n");
	}
	if (!mysql_real_connect(&mysql3, "localhost", "root", "dhn15158882311", sqlName3, 3306, NULL, 0))	//connect to test2011cp
	{
		fprintf(stderr, "%s\n", mysql_error(&mysql3));											//prototype   const char *mysql_error(MYSQL *mysql)
		exit(1);
	}
	else
	{
		fprintf(stderr, "mysql3 connect to user_word_pre successfully!\n");
	}

	if (mysql_query(&mysql1, "set names gbk"))												//change character set
	{
		fprintf(stderr, "error!,%s\n", mysql_error(&mysql1));
		exit(1);
	}
	if (mysql_query(&mysql2, "set names gbk"))												//change character set
	{
		fprintf(stderr, "error!,%s\n", mysql_error(&mysql1));
		exit(1);
	}
	if (mysql_query(&mysql3, "set names gbk"))												//change character set
	{
		fprintf(stderr, "error!,%s\n", mysql_error(&mysql1));
		exit(1);
	}

	if ( mysql_set_character_set( &mysql1 , "gbk" ))		//change the character set
	{
		fprintf(stderr, "error,%s\n", mysql_error(&mysql1));
	}
	if (mysql_set_character_set(&mysql2, "gbk"))		//change the character set
	{
		fprintf(stderr, "error,%s\n", mysql_error(&mysql1));
	}
	if (mysql_set_character_set(&mysql3, "gbk"))		//change the character set
	{
		fprintf(stderr, "error,%s\n", mysql_error(&mysql1));
	}


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
        char select[50];
        sprintf(select, "select word from user_words_pre.%s", userIdRow[0]);
        MYSQL_RES *words_res = queryOrder1(&mysql2, select);
        MYSQL_ROW  word_row;
        string tem(userIdRow[0]);
        curUser.user_id = tem;

//        //Ϊÿ���û�����
//        char createTable[50];
//        sprintf(createTable, "create table user_friends.%s(friends varchar(20))", userIdRow[0]);
//        queryOrder2(&mysql3, createTable);

        while ((word_row = mysql_fetch_row(words_res)))                 //��ȡÿ���ִ�
        {
            curUser.words.push_back(word_row[0]);
        }
        allUser.push_back(curUser);

        counter++;
    }//while

    cout << "���������ھ�\n";
    //Ѱ�������ھ�  ʱ�临�Ӷ�:O(n2)
    int i = 0;
    for (auto a_iter = allUser.begin(); a_iter != allUser.end(); ++a_iter, i++)        //Ϊÿ���û�aѰ�������ھ�
    {
        if (!(i % 50))
            printf("%d\n", i);
        vector<userComNum> userComNumVec;
        for (auto b_iter = allUser.begin(); b_iter != allUser.end(); ++b_iter)      //��������ÿ���û�b�Ա�
        {
            userComNum b_user;
            if (a_iter != b_iter)
            {
                int commonWdNum = calc_common_words(*a_iter, *b_iter);        //���������û���ͬ�ķִ���

                //    **********�������ƶ�***************

                b_user.userId = b_iter->user_id;
                b_user.commonNum = commonWdNum;
//                printf("%s-%s:%d common words\n", (a_iter->user_id).c_str(), (b_user.userId).c_str(), b_user.commonNum);
//                system("pause");

            }
            userComNumVec.push_back(b_user);
        }

        sort(userComNumVec.begin(), userComNumVec.end(),
             [] (const userComNum &u1, const userComNum &u2) {return u1.commonNum > u2.commonNum;});    //����a��ͬ�ִ�����������
        for (auto it = userComNumVec.begin(); it != userComNumVec.begin() + 10; ++it)    //��ǰʮ����ͬ�ִ������û�����a��friends��
        {
            a_iter->friends.push_back(it->userId);
        }

        //cout << ++i << endl;
    }//end for

    //д�����ݿ�
    char insert[50];
    for (auto iter = allUser.begin(); iter != allUser.end(); ++iter)
    {
        for (auto it = iter->friends.begin(); it != iter->friends.end(); ++it)
        {
            sprintf(insert, "insert into user_friends.%s values (%s)", (iter->user_id).c_str(), (*it).c_str());
            queryOrder2(&mysql3, insert);
        }
    }//end for


    cout << counter << endl;

	mysql_close(&mysql1);
	mysql_close(&mysql2);

    return 0;

}

