#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sys/time.h>
#include <isa-l.h>
#include "libmemcached/memcached.h"

#include <iostream>
#include <vector>
#include <queue>
#include <map>
#include <algorithm>
#include <iomanip>

using namespace std;
//g++ -std=c++11 update.cpp -o update -lmemcached -lisal

unsigned int N, K, SERVER, CHUNK_SIZE;
char path[100] = {0}; //path
char IP[100] = "127.0.0.1"; // node server

static double timeval_diff(struct timeval *start,
                           struct timeval *end)
{
    double r = end->tv_sec - start->tv_sec;
    if (end->tv_usec > start->tv_usec)
        r += (end->tv_usec - start->tv_usec) / 1000000.0;
    else if (end->tv_usec < start->tv_usec)
        r -= (start->tv_usec - end->tv_usec) / 1000000.0;
    return r;
}

struct request_st
{
    int op; //read=0,update=1
    string key;
};

vector<string> vload;
vector<struct request_st> vrun;

vector<memcached_st *> memc(N - K); //data +XOR,P2,P3
//the buffers of each memcached;
vector<queue<string> > vbuf(SERVER);

unsigned int StripID = 0;
unsigned int encode_inc = 0;  //for random encoding
unsigned int delta_inc = 0;   //for all parity delta
unsigned int version_inc = 0; //for all version data

//stripe metadata
map<string, pair<unsigned int, int> > object_index; //key -> stripID, offset

//stripe ID -> keys+flag(flag mark for valid or not), invaild=1;
vector<vector<pair<string, int> > > stripe_index;

double time_all = 0;
int update_all = 0;

//IO times
unsigned int set_all = 0;
unsigned int get_all = 0;
unsigned int del_all = 0;
unsigned int dram_all = 0;
unsigned int disk_all = 0;

double time_encode = 0;
int delete_all = 0;

//0 -> can not, 1->can
int check_encode(vector<string> &dd)
{
    int count = 0;
    int inc1 = encode_inc;
    for (int i = 0; i < SERVER; i++)
    {
        if (!vbuf[inc1 % SERVER].empty())
        {
            count++;
            if (count == K)
            {
                cout << "\tcan encode!" << endl;
                break;
            }
        }
        inc1++;
    }
    //round_rabin the start tranverse point
    encode_inc++;

    //can encode
    if (count == K)
    {
        int inc2 = encode_inc - 1;
        for (int i = 0; count > 0 && i < SERVER; i++)
        {
            if (!vbuf[inc2 % SERVER].empty())
            {
                //dd keys
                cout << "\t\t" << vbuf[inc2 % SERVER].front() << endl;
                dd.push_back(vbuf[inc2 % SERVER].front());
                vbuf[inc2 % SERVER].pop();
                count--;
            }
            inc2++;
        }
        return 1;
    }
    else
        return 0;
}

void encode(vector<string> &dd, vector<string> &pp)
{
    unsigned char *data[K];
    unsigned char *parity[N - K];

    for (int i = 0; i < K; i++)
    {
        data[i] = new unsigned char[CHUNK_SIZE];
        memset(data[i], dd[i][5] - '0' + 'a', CHUNK_SIZE * sizeof(char));
        //memcpy(data[i],dd[i].data(),CHUNK_SIZE);
    }

    for (int i = 0; i < N - K; i++)
    {
        parity[i] = new unsigned char[CHUNK_SIZE];
    }

    unsigned char encode_gftbl[32 * K * (N - K)];
    unsigned char encode_matrix[N * K];

    gf_gen_rs_matrix(encode_matrix, N, K);
    ec_init_tables(K, N - K, &(encode_matrix[K * K]), encode_gftbl);

    ec_encode_data(CHUNK_SIZE, K, N - K, encode_gftbl, data, parity);

    for (int i = 0; i < K; i++)
    {
        delete[] data[i];
    }

    for (int i = 0; i < N - K; i++)
    {
        pp[i] = (const char *)parity[i];
        delete[] parity[i];
    }

    cout << "\tENCODE OK" << endl;
}

int init()
{
    char p1[100] = {0}, p2[100] = {0};
    sprintf(p1, "./%s/%s", path, "ycsb_set.txt");
    FILE *fin_load = fopen(p1, "r");
    sprintf(p2, "./%s/%s", path, "ycsb_test.txt");
    FILE *fin_run = fopen(p2, "r");

    if (!(fin_load || fin_run))
        cerr << "Load or Run file error!" << endl;
    char tmp[1024];
    while (fgets(tmp, 1024, fin_load) && (!feof(fin_load)))
    {
        char key[250] = {0};
        if (sscanf(tmp, "INSERT %s", key))
        {
            vload.push_back(string(key));
        }
    }

    struct request_st rr;
    while (fgets(tmp, 1024, fin_run) && (!feof(fin_run)))
    {
        char key[250] = {0};
        if (sscanf(tmp, "READ %s", key))
        {
            rr.op = 0;
            rr.key = string(key);
            vrun.push_back(rr);
        }
        if (sscanf(tmp, "UPDATE %s", key))
        {
            rr.op = 1;
            rr.key = string(key);
            vrun.push_back(rr);
        }
    }

    fclose(fin_load);
    fclose(fin_run);
    cout << "FILE LOAD OK" << endl;

    //Add server
    memcached_return rc;
    memc.resize(N - K);
    vbuf.resize(SERVER);

    memc[0] = memcached_create(NULL);
    memc[1] = memcached_create(NULL);
    memc[2] = memcached_create(NULL);
    memc[3] = memcached_create(NULL);

    //data+P1
    for(int i=0; i<SERVER;i++)
    {
        rc = memcached_server_add(memc[0], IP, 11211+i);
    }
    //parity
    for(int i=0; i<N-K-1;i++)
    {
        rc = memcached_server_add(memc[i+1], IP, 20000+i);
    }
    cout << "ADD SERVER OK" << endl;

    //load data, w/o implemetation metadata
    for (int i = 0; i < vload.size(); i++)
    {
        //return index;
        char value[CHUNK_SIZE + 1] = {0};
        //random
        memset(value, vload[i][5] - '0' + 'a', CHUNK_SIZE * sizeof(char));
        //cout << value <<endl;
        unsigned int index = 0;
        rc = memcached_set_index(memc[0], vload[i].data(), vload[i].length(), value, CHUNK_SIZE, 0, 0, (uint32_t *)&index);

        if (rc == MEMCACHED_SUCCESS)
        {
            vbuf[index].push(string(vload[i]));
            cout << i << ".Put " << vload[i] << " (" << vload[i].length() << ") into " << index << " buffer" << endl;

            vector<string> dd;
            if (check_encode(dd) == 1)
            {
                //encode
                vector<string> pp(N - K);

                encode(dd, pp);

                //store XOR & non-XOR parity chunks
                for (int j = 0; j < N - K; j++)
                {
                    char p1[100] = {0};
                    sprintf(p1, "SID%u-P%d", StripID, j + 1);
                    //todo, need consider the distribution of XOR parity
                    rc = memcached_set(memc[j], p1, strlen(p1), pp[j].data(), CHUNK_SIZE, 0, 0);
                }

                cout << "\tPARITY SET OK" << endl;
                //put into Object Index
                //###key->stripe ID###
                for (int j = 0; j < dd.size(); j++)
                {
                    //key->stripeID, offset
                    object_index.insert(pair<string, pair<unsigned int, int> >(dd[j], pair<unsigned int, int>(StripID, j)));
                }

                //put into Stripe Index, only data, parity can be calculated
                //###StripeID -> keys+flag###
                vector<pair<string, int> > ddd;
                for (int i = 0; i < dd.size(); i++)
                {
                    ddd.push_back(pair<string, int>(dd[i], 0)); //valid, flag=0, for batch coding;
                }

                stripe_index.push_back(ddd);

                cout << "\tMETADTA OK" << endl;

                StripID++;
            }
        }
    }
}

void run_inplace()
{
    cerr << endl << "[In-place]";
    unsigned char encode_gftbl[32 * K * (N - K)];
    unsigned char encode_matrix[N * K];

    gf_gen_rs_matrix(encode_matrix, N, K);
    ec_init_tables(K, N - K, &(encode_matrix[K * K]), encode_gftbl);

    //XOR
    unsigned char encode_gftbl_xor[32 * 2 * 1];
    unsigned char encode_matrix_xor[3 * 2];

    gf_gen_rs_matrix(encode_matrix_xor, 3, 2);
    ec_init_tables(2, 1, &(encode_matrix_xor[2 * 2]), encode_gftbl_xor);

    struct timeval begin, end;
    for (int i = 0; i < vrun.size(); i++)
    {
        if (vrun[i].op == 0) //read, skip
            continue;
        else
        {
            update_all++;
            double time_single = 0;

            size_t val_len;
            uint32_t flags;
            memcached_return rc;

            cout << endl
                 << "[" << i << "].Start update (" << vrun[i].key << ")" << endl;
            //1. retrieve old
            char *old_data = memcached_get(memc[0], vrun[i].key.data(), vrun[i].key.length(), &val_len, &flags, &rc);

            if (old_data)
                cout << "\t1.RETRIEVE OLD DATA OK, (" << old_data[0] << ")" << endl;
            else
                old_data = new char[CHUNK_SIZE];

            //2. set new value
            char new_data[CHUNK_SIZE] = {0};
            memset(new_data, 'N', CHUNK_SIZE * sizeof(char));
            rc = memcached_set(memc[0], vrun[i].key.data(), vrun[i].key.length(), new_data, CHUNK_SIZE, 0, 0);

            cout << "\t2.SET NEW VALUE OK, (" << new_data[0] << ")" << endl;

            gettimeofday(&begin, 0);
            //3. create data deltas
            unsigned char *delta_data = new unsigned char[CHUNK_SIZE];
            unsigned char *delta_parity[N - K];
            unsigned char *new_parity[N - K];

            //data delta
            unsigned char *d[2];
            d[0] = (unsigned char *)old_data;
            d[1] = (unsigned char *)new_data;

            ec_encode_data(CHUNK_SIZE, 2, 1, encode_gftbl_xor, d, &delta_data);
            cout << "\t3.CREATE DATA DELTA OK, [" << (unsigned int)delta_data[0] << "]" << endl;

            //4. create parity deltas
            for (int j = 0; j < N - K; j++)
            {
                delta_parity[j] = new unsigned char[CHUNK_SIZE];
                new_parity[j] = new unsigned char[CHUNK_SIZE];
            }

            //obtain offset
            ec_encode_data_update(CHUNK_SIZE, K, N - K, object_index[vrun[i].key.data()].second, encode_gftbl, delta_data, delta_parity);

            cout << "\t4.CREATE PARITY DELTAS OK, ";
            for (int j = 0; j < N - K; j++)
                cout << "[" << (unsigned int)delta_parity[j][0] << "] ";
            cout << endl;

            //5. retrieve parity chunk
            char *old_parity[N - K];

            unsigned int sid = object_index[vrun[i].key.data()].first;

            //XOR & non-XOR parity chunks
            for (int j = 0; j < N - K; j++)
            {
                char p1[100] = {0};
                sprintf(p1, "SID%u-P%d", sid, j + 1);
                old_parity[j] = memcached_get(memc[j], p1, strlen(p1), &val_len, &flags, &rc);
                get_all++;
                if (old_parity[j])
                    cout << "old P" << j + 1 << ":[" << (int)old_parity[j][0] << "]; ";
                else
                    old_parity[j] = new char[CHUNK_SIZE];
            }

            cout << endl
                 << "\t5.RETRIEVE OLD PARITY OK" << endl;

            //6. create all new parity,XOR operation, K=2, N=3
            for (int j = 0; j < N - K; j++)
            {
                unsigned char *p[2];
                p[0] = (unsigned char *)old_parity[j];
                p[1] = delta_parity[j];

                ec_encode_data(CHUNK_SIZE, 2, 1, encode_gftbl_xor, p, &(new_parity[j]));
            }

            cout << "\t6.CREATE NEW PARITY OK" << endl
                 << "\t\t";
            for (int j = 0; j < N - K; j++)
                cout << "new P" << j + 1 << ":[" << (int)new_parity[j][0] << "]; ";
            cout << endl;

            //7. set new parity, in-place
            //store non-XOR parity chunks
            for (int j = 0; j < N - K; j++)
            {
                char p1[100] = {0};
                sprintf(p1, "SID%u-P%d", sid, j + 1);
                rc = memcached_set(memc[j], p1, strlen(p1), (char *)new_parity[j], CHUNK_SIZE, 0, 0);
                set_all++;
            }

            cout << "\t7.SET NEW PARITY OK" << endl;

            gettimeofday(&end, 0);
            time_single = timeval_diff(&begin, &end);
            time_all += time_single;

            cout << "Time: " << time_single << " s" << endl;

            for (int j = 0; j < N - K; j++)
            {
                delete[] delta_parity[j];
                delete[] new_parity[j];
            }

            delete[] delta_data;
        }
    }
}

void run_hybrid()
{
    cerr << endl << "[LogECMem]";
    unsigned char encode_gftbl[32 * K * (N - K)];
    unsigned char encode_matrix[N * K];

    gf_gen_rs_matrix(encode_matrix, N, K);
    ec_init_tables(K, N - K, &(encode_matrix[K * K]), encode_gftbl);

    //XOR
    unsigned char encode_gftbl_xor[32 * 2 * 1];
    unsigned char encode_matrix_xor[3 * 2];

    gf_gen_rs_matrix(encode_matrix_xor, 3, 2);
    ec_init_tables(2, 1, &(encode_matrix_xor[2 * 2]), encode_gftbl_xor);

    struct timeval begin, end;
    //run test
    for (int i = 0; i < vrun.size(); i++)
    {
        if (vrun[i].op == 0) //read, skip
            continue;
        else
        {
            update_all++;
            double time_single = 0;

            size_t val_len;
            uint32_t flags;
            memcached_return rc;

            cout << endl
                 << "[" << i << "].Start update (" << vrun[i].key << ")" << endl;
            //1. retrieve old
            char *old_data = memcached_get(memc[0], vrun[i].key.data(), vrun[i].key.length(), &val_len, &flags, &rc);

            if (old_data)
                cout << "\t1.RETRIEVE OLD DATA OK, (" << old_data[0] << ")" << endl;
            else
                old_data = new char[CHUNK_SIZE];
            //2. set new value
            char new_data[CHUNK_SIZE] = {0};
            memset(new_data, 'N', CHUNK_SIZE * sizeof(char));
            rc = memcached_set(memc[0], vrun[i].key.data(), vrun[i].key.length(), new_data, CHUNK_SIZE, 0, 0);

            cout << "\t2.SET NEW VALUE OK, (" << new_data[0] << ")" << endl;

            gettimeofday(&begin, 0);
            //3. create data deltas
            unsigned char *delta_data = new unsigned char[CHUNK_SIZE];
            unsigned char *delta_parity[N - K];

            //data delta
            unsigned char *d[2];
            d[0] = (unsigned char *)old_data;
            d[1] = (unsigned char *)new_data;

            ec_encode_data(CHUNK_SIZE, 2, 1, encode_gftbl_xor, d, &delta_data);
            cout << "\t3.CREATE DATA DELTA OK, [" << (unsigned int)delta_data[0] << "]" << endl;

            //4. create parity deltas
            for (int j = 0; j < N - K; j++)
            {
                delta_parity[j] = new unsigned char[CHUNK_SIZE];
            }

            //obtain offset
            ec_encode_data_update(CHUNK_SIZE, K, N - K, object_index[vrun[i].key.data()].second, encode_gftbl, delta_data, delta_parity);

            cout << "\t4.CREATE PARITY DELTAS OK, ";
            for (int j = 0; j < N - K; j++)
                cout << "[" << (unsigned int)delta_parity[j][0] << "] ";
            cout << endl;

            //5. retrieve XOR
            char *old_parity;

            //XOR parity
            char p1[100] = {0};
            //obtain stripeID
            unsigned int sid = object_index[vrun[i].key.data()].first;
            sprintf(p1, "SID%u-P%d", sid, 1);
            old_parity = memcached_get(memc[0], p1, strlen(p1), &val_len, &flags, &rc);
            get_all++;

            if (old_parity)
                cout << "\t\told P1:[" << (int)old_parity[0] << "]; ";
            else
                old_parity = new char[CHUNK_SIZE];

            cout << endl
                 << "\t5.RETRIEVE OLD XOR PARITY OK" << endl;

            //6. create new XOR parity
            unsigned char *new_parity = new unsigned char[CHUNK_SIZE];
            unsigned char *p[2];
            p[0] = (unsigned char *)old_parity;
            p[1] = delta_parity[0];

            ec_encode_data(CHUNK_SIZE, 2, 1, encode_gftbl_xor, p, &(new_parity));

            cout << "\t6.CREATE XOR NEW PARITY OK" << endl
                 << "\t\t";
            cout << "new P1:[" << (int)new_parity[0] << "]; " << endl;

            //7. set XOR & parity delta,
            //Todo, need extra metadata
            //store XOR parity, in-place
            memset(p1, '\0', 100 * sizeof(char));
            sprintf(p1, "SID%u-P%d", sid, 1);
            rc = memcached_set(memc[0], p1, strlen(p1), (char *)new_parity, CHUNK_SIZE, 0, 0);
            set_all++;

            //store non-XOR parity delta
            for (int j = 1; j < N - K; j++)
            {
                char p2[100] = {0};
                sprintf(p2, "[Delta-%u]SID%u-P%d", delta_inc++, sid, j + 1);
                //todo, need consider the distribution of XOR parity
                rc = memcached_set(memc[j], p2, strlen(p2), (char *)delta_parity[j], CHUNK_SIZE, 0, 0);
                set_all++;
                disk_all++;
            }

            cout << "\t7.SET DELTAS PARITY OK" << endl;

            gettimeofday(&end, 0);
            time_single = timeval_diff(&begin, &end);
            time_all += time_single;

            cout << "Time: " << time_single << " s" << endl;

            for (int j = 0; j < N - K; j++)
            {
                delete[] delta_parity[j];
            }

            delete[] delta_data;
            delete[] new_parity;
        }
    }
}

//for back stripe
int head = 0;
const char *find_object(int back)
{
    static int h = 0;
    for (; head < back && head < stripe_index.size(); head++)
    {
        for (; h < stripe_index[head].size(); h++)
        {
            if (stripe_index[back][h].second == 0) //non-empty
            {
                cout << "Find non-empty" << endl;
                h++;
                return stripe_index[back][h - 1].first.data();
            }
        }

        //delete the parity of the stripe
        cout << "Delete stripe, SID=" << head << " (" << stripe_index[head].size() << ")" << endl;
        delete_all++;

        for (int i = 0; i < N - K; i++)
        {
            char p1[100] = {0};
            sprintf(p1, "SID%u-P%d", head, i + 1);
            memcached_delete(memc[i], p1, strlen(p1), 0);
            del_all++;
        }

        h = 0; //back to 0
    }
    if (head >= back)
        return NULL;
}

void run_full()
{
    cerr << endl << "[Full-Stripe]";
    unsigned char encode_gftbl[32 * K * (N - K)];
    unsigned char encode_matrix[N * K];

    gf_gen_rs_matrix(encode_matrix, N, K);
    ec_init_tables(K, N - K, &(encode_matrix[K * K]), encode_gftbl);

    unsigned char encode_gftbl_xor[32 * 2 * 1];
    unsigned char encode_matrix_xor[3 * 2];

    gf_gen_rs_matrix(encode_matrix_xor, 3, 2);
    ec_init_tables(2, 1, &(encode_matrix_xor[2 * 2]), encode_gftbl_xor);

    struct timeval begin, end;
    //run test
    for (int i = 0; i < vrun.size(); i++)
    {
        if (vrun[i].op == 0) //read, skip
            continue;
        else
        {
            update_all++;
            //normal encode
            char key[100] = {0}; //+version
            sprintf(key, "[update-%u]%s", version_inc++, vrun[i].key.data());
            char value[CHUNK_SIZE + 1] = {0};
            //random
            memset(value, key[5] - '0' + 'a', CHUNK_SIZE * sizeof(char));
            //cout << value <<endl;
            unsigned int index = 0;
            memcached_return rc = memcached_set_index(memc[0], key, strlen(key), value, CHUNK_SIZE, 0, 0, (uint32_t *)&index);
            dram_all++;

            gettimeofday(&begin, 0);
            if (rc == MEMCACHED_SUCCESS)
            {
                //put into buffer
                vbuf[index].push(string(key));
                cout << "Update " << i << ".Put " << key << " (" << strlen(key) << ") into " << index << " buffer" << endl;

                //make the elder object's flag ==1;
                unsigned int sid_older = object_index[vrun[i].key.data()].first;
                int offset = object_index[vrun[i].key.data()].second;

                cout << sid_older << " " << offset << " need to mark=1" << endl;

                //invaild
                stripe_index[sid_older][offset].second = 1;

                vector<string> dd;
                if (check_encode(dd) == 1)
                {
                    //encode
                    vector<string> pp(N - K);

                    encode(dd, pp);

                    //store XOR & non-XOR parity chunks
                    for (int j = 0; j < N - K; j++)
                    {
                        char p1[100] = {0};
                        sprintf(p1, "SID%u-P%d", StripID, j + 1);
                        //todo, need consider the distribution of XOR parity
                        rc = memcached_set(memc[j], p1, strlen(p1), pp[j].data(), CHUNK_SIZE, 0, 0);
                        set_all++;
                        dram_all++;
                    }

                    cout << "\tPARITY SET OK" << endl;
                    //put into Object Index
                    //###key->stripe ID###
                    for (int j = 0; j < dd.size(); j++)
                    {
                        //key->stripeID, offset
                        if (dd[j].substr(0, 8).compare("[update-") == 0) //updated key
                        {
                            //prefix "user"
                            int pos = dd[j].find("user", 0);
                            object_index[dd[j].substr(pos)] = pair<unsigned int, int>(StripID, j); //overlap former
                            //object_index.insert(pair<string, pair<unsigned int, int> >(dd[j].substr(pos), pair<unsigned int, int>(StripID, j)));
                        }
                        else
                            object_index.insert(pair<string, pair<unsigned int, int> >(dd[j], pair<unsigned int, int>(StripID, j)));
                    }

                    //put into Stripe Index, only data, parity can be calculated
                    //###StripeID -> keys+flag###
                    vector<pair<string, int> > ddd;
                    for (int i = 0; i < dd.size(); i++)
                    {
                        ddd.push_back(pair<string, int>(dd[i], 0)); //valid, flag=0;
                    }

                    stripe_index.push_back(ddd);

                    cout << "\tMETADTA OK, SID= " << StripID << endl;

                    StripID++;
                }
            }
            gettimeofday(&end, 0);
            cout << "Time of encoding new stripes: " << timeval_diff(&begin, &end) << endl;
            time_encode = timeval_diff(&begin, &end);
            time_all += time_encode;
        }
    }

    size_t val_len;
    uint32_t flags;
    memcached_return rc;

    //GC module
    gettimeofday(&begin, 0);
    cout << endl
         << "GC Start!" << endl
         << endl;
    int back = 0;
    for (back = stripe_index.size() - 1; head < back && head < stripe_index.size() && back > 0; back--)
    {
        //find an empty stripe backfoward
        vector<vector<unsigned char *> > vdelta(N - K); //each update a list of delta for p1,p2,p3
        for (int i = 0; i < stripe_index[back].size(); i++)
        {
            if (stripe_index[back][i].second == 1) //empty, need replaced
            {
                char *old_data = memcached_get(memc[0], stripe_index[back][i].first.data(), stripe_index[back][i].first.length(), &val_len, &flags, &rc);
                get_all++;

                if (!old_data)
                    old_data = new char[CHUNK_SIZE];

                //need delete the old data;
                memcached_delete(memc[0], stripe_index[back][i].first.data(), stripe_index[back][i].first.length(), 0);
                del_all++;

                //find a non-empty
                const char *key = find_object(back);
                if (key == NULL)
                {
                    cout << endl
                         << "GC OVER! Delete Stripe " << delete_all << endl;
                    break;
                }
                else
                {
                    char *new_data = memcached_get(memc[0], key, strlen(key), &val_len, &flags, &rc);
                    get_all++;

                    if (!new_data)
                        new_data = new char[CHUNK_SIZE];

                    unsigned char *delta_data = new unsigned char[CHUNK_SIZE];
                    unsigned char *delta_parity[N - K];
                    //data delta
                    unsigned char *d[2];
                    d[0] = (unsigned char *)old_data;
                    d[1] = (unsigned char *)new_data;
                    ec_encode_data(CHUNK_SIZE, 2, 1, encode_gftbl_xor, d, &delta_data);
                    cout << "\tCREATE DATA DELTAS OK, "
                         << "for " << stripe_index[back][i].first.data() << endl;

                    //parity delta
                    for (int a = 0; a < N - K; a++)
                    {
                        delta_parity[a] = new unsigned char[CHUNK_SIZE];
                    }

                    ec_encode_data_update(CHUNK_SIZE, K, N - K, object_index[vrun[i].key.data()].second, encode_gftbl, delta_data, delta_parity);
                    cout << "\tCREATE PARITY DELTAS OK" << endl;

                    for (int a = 0; a < N - K; a++)
                    {
                        vdelta[a].push_back(delta_parity[a]);
                    }

                    //free deltas
                    delete[] delta_data;
                    for (int i = 0; i < N - K; i++)
                    {
                        delete[] delta_parity[i];
                    }
                }
            }
        }

        //in-place
        if (vdelta[0].size()) //not empty
        {
            //retrieve parity chunk
            char *old_parity[N - K];

            //XOR & non-XOR parity chunks
            for (int i = 0; i < N - K; i++)
            {
                char p1[100] = {0};
                sprintf(p1, "SID%u-P%d", back, i + 1);
                old_parity[i] = memcached_get(memc[i], p1, strlen(p1), &val_len, &flags, &rc);
                get_all++;
                if (old_parity[i])
                    cout << "old P" << i + 1 << ":[" << (int)old_parity[i][0] << "]; ";
                else
                    old_parity[i] = new char[CHUNK_SIZE];
            }

            cout << endl
                 << "\tRETRIEVE OLD PARITY OK" << endl;

            //need multiple XOR calculation for all paritys
            unsigned char *new_parity[N - K];

            //create all new parity,XOR old & delta1, K=2, N=3
            for (int i = 0; i < N - K; i++)
            {
                new_parity[i] = new unsigned char[CHUNK_SIZE];
                unsigned char *p[2];
                p[0] = (unsigned char *)old_parity[i];
                p[1] = (unsigned char *)vdelta[i][0];

                ec_encode_data(CHUNK_SIZE, 2, 1, encode_gftbl_xor, p, &(new_parity[i]));
            }

            cout << "\tCREATE NEW PARITY OK" << endl;

            //further delta
            for (int i = 0; i < N - K; i++)
            {
                for (int j = 1; j < vdelta[0].size(); j++)
                {
                    unsigned char *p[2];
                    memcpy(old_parity[i], new_parity[i], CHUNK_SIZE * sizeof(unsigned char));
                    p[0] = (unsigned char *)old_parity[i];
                    p[1] = (unsigned char *)vdelta[i][j];

                    ec_encode_data(CHUNK_SIZE, 2, 1, encode_gftbl_xor, p, &(new_parity[i]));
                }
            }

            cout << "\tCREATE FURTHER NEW PARITY OK" << endl
                 << "\t\t";
            for (int i = 0; i < N - K; i++)
                cout << "new P" << i + 1 << ":[" << (int)new_parity[i][0] << "]; ";
            cout << endl;

            //store XOR & non-XOR parity chunks
            for (int i = 0; i < N - K; i++)
            {
                char p1[100] = {0};
                sprintf(p1, "SID%u-P%d", back, i + 1);
                //todo, need consider the distribution of XOR parity
                rc = memcached_set(memc[i], p1, strlen(p1), (char *)new_parity[i], CHUNK_SIZE, 0, 0);
                set_all++;
            }

            cout << "\tSET NEW PARITY OK" << endl;

            //free vdeltas
            for (int i = 0; i < N - K; i++)
            {
                delete[] new_parity[i];
            }
        }
    }
    if (head >= back)
        cerr << "GC OVER! Delete Stripe " << delete_all << endl;

    gettimeofday(&end, 0);
    cerr << "Time of encoding: " << time_encode << endl;
    cerr << "Time of GC: " << timeval_diff(&begin, &end);
    time_all += timeval_diff(&begin, &end);
}

//./update op path N K
int main(int argc, char *argv[])
{
    //para config
    N = 14;
    K = 10;
    SERVER = 16;
    CHUNK_SIZE = 4096; //4K

    // if (argc < 1)
    // {
    //     //./update 1 10K95z
    //     printf("Command Wrong!\n\n");
    //     exit(-1);
    // }

    sscanf(argv[2], "%s", path);
    sscanf(argv[3], "%d", &N);
    sscanf(argv[4], "%d", &K);
    sscanf(argv[5], "%s", IP);

    init();

    int op = 0;
    sscanf(argv[1], "%d", &op);

    if (op == 1)
        run_inplace();
    else if (op == 2)
        run_full();
    else if (op == 3)
        run_hybrid();
    else
        ;

    cerr << "ALL Time: " << time_all << " s" << endl;
    cerr << "ALL Update: " << update_all << endl;
    cerr << "AVG time: " << fixed << setprecision(1) << time_all / update_all * 1000000.0 << " us" << endl;
    cerr << "SET: " << set_all << "; GET: " << get_all << "; DEL: " << del_all << "; ALL IOs: " << set_all + get_all + del_all << endl;
    cerr << "DRAM Extra Cost: " << dram_all << endl;
    //cerr << "Disk Extra Cost: " << disk_all << endl;
    return 0;
}