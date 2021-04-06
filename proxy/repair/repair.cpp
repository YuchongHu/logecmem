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
//g++ -std=c++11 repair.cpp -o repair -lmemcached -lisal

unsigned int N, K, SERVER, CHUNK_SIZE;
char path[100] = {0};           //path
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
double time_dram_all = 0;
double time_disk_all = 0;
int degraded_all = 0;

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

    char tmp[1024];
    while (fgets(tmp, 10240, fin_load) && (!feof(fin_load)))
    {
        char key[250] = {0};
        if (sscanf(tmp, "INSERT %s", key))
        {
            vload.push_back(string(key));
        }
    }

    struct request_st rr;
    while (fgets(tmp, 10240, fin_run) && (!feof(fin_run)))
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
            //put into buffer
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

void run()
{
    size_t val_len;
    uint32_t flags;
    memcached_return rc;
    struct timeval begin, end, bb, ee;
    //run test
    int count = 1;
    while (count--)
    {
        for (int i = 0; i < vrun.size(); i++)
        {
            if (vrun[i].op == 1) //update, skip
                continue;
            else
            {
                degraded_all++;
                gettimeofday(&begin, 0);
                cout << "Degraded reads [" << i << "](" << vrun[i].key << ")" << endl;

                //full DRAM
                auto iter = object_index.find(vrun[i].key);

                unsigned int sid = 0, offset = 0;
                if (iter != object_index.end())
                {
                    cout << "Encoded" << endl;
                    sid = (iter->second).first;
                    offset = (iter->second).second;
                }
                else
                {
                    cout << "Not encoded" << endl;
                    continue;
                }

                cout << "\tsid= " << sid << " offset= " << offset << endl;

                int err_arr[2] = {-1, -1};
                err_arr[0] = offset; //itself
                if (offset == 0)
                {
                    err_arr[1] = 1;
                }
                else
                {
                    err_arr[1] = offset - 1; //assump front & itself are failed;
                }

                cout << "Assume " << err_arr[0] << " and " << err_arr[1] << " fail\n";

                gettimeofday(&bb, 0);
                //DRAM
                char *left_data[K];
                int in = 0;
                for (int j = 0; j < K; j++)
                {
                    if (j == err_arr[0] || j == err_arr[1])
                        continue;
                    left_data[in++] = memcached_get(memc[0], stripe_index[sid][j].first.c_str(), stripe_index[sid][j].first.length(), &val_len, &flags, &rc);
                    if (rc == MEMCACHED_SUCCESS)
                        cout << "\tRead Success " << stripe_index[sid][j].first.c_str() << endl;
                }
                cout << "Read DRAM DATA OVER!" << endl;

                //XOR parity
                char p1[100] = {0};
                sprintf(p1, "SID%u-P%d", sid, 1);
                left_data[in++] = memcached_get(memc[0], p1, strlen(p1), &val_len, &flags, &rc);
                if (rc == MEMCACHED_SUCCESS)
                    cout << "Read DRAM XOR OVER!" << endl;

                gettimeofday(&ee, 0);
                time_dram_all += timeval_diff(&bb, &ee);

                gettimeofday(&bb, 0);
                //disk
                FILE *fin = fopen("4K.txt", "r");
                char tmp[102400] = {0};
                if (fin)
                {
                    fgets(tmp, 102400, fin);
                    left_data[in] = new char[CHUNK_SIZE];
                    memcpy(left_data[in], tmp, CHUNK_SIZE);
                    fclose(fin);
                    cout << "Read Disk P2 OVER! " << left_data[in] << endl;
                }
                else
                    cerr << "FILE OPEN FAILED" << endl;

                gettimeofday(&ee, 0);
                time_disk_all += timeval_diff(&bb, &ee);

                unsigned char encode_matrix[N * K] = {0};
                unsigned char error_matrix[N * K] = {0};
                unsigned char invert_matrix[N * K] = {0};
                unsigned char decode_matrix[N * K] = {0};

                for (int j = 0, r = 0; j < N; j++)
                {
                    if (j == err_arr[0] || j == err_arr[1])
                        continue;
                    for (int a = 0; a < K; a++)
                        error_matrix[r * K + a] = encode_matrix[j * K + a];
                    r++;
                }

                gf_invert_matrix(error_matrix, invert_matrix, K);

                cout << "Error Matrix" << endl;

                for (int e = 0; e < 2; e++)
                {
                    int idx = err_arr[e];
                    if (idx < K) // We lost one of the buffers containing the data
                    {
                        for (int j = 0; j < K; j++)
                            decode_matrix[e * K + j] = invert_matrix[idx * K + j];
                    }
                    else // We lost one of the buffer containing the error correction codes
                    {
                        for (int j = 0; j < K; j++)
                        {
                            unsigned char s = 0;
                            //mul the encode matrix coefficient to get the failed data only
                            for (int a = 0; a < K; a++)
                                s ^= gf_mul(invert_matrix[a * K + j], encode_matrix[idx * K + a]);
                            decode_matrix[e * K + j] = s;
                        }
                    }
                }

                cout << "Decode Matrix" << endl;

                unsigned char decode_gftbl[32 * K * (N - K)];
                unsigned char *recovery_data[2];

                for (int j = 0; j < 2; j++)
                    recovery_data[j] = new unsigned char[CHUNK_SIZE];

                for (int j = 0; j < K; j++)
                {
                    left_data[j] = new char[CHUNK_SIZE];
                    memset(left_data[j], '1', CHUNK_SIZE * sizeof(char));
                }

                ec_init_tables(K, N - K, decode_matrix, decode_gftbl);
                ec_encode_data(CHUNK_SIZE, K, 2, decode_gftbl, (unsigned char **)left_data, recovery_data);

                cout << "Decode OVER" << endl
                     << endl;

                for (int j = 0; j < 2; j++)
                    delete[] recovery_data[j];
                delete left_data[in];

                gettimeofday(&end, 0);
                time_all += timeval_diff(&begin, &end);
            }
        }
    }
}

void run_plr()
{
    size_t val_len;
    uint32_t flags;
    memcached_return rc;
    struct timeval begin, end, bb, ee;
    int count = 1;
    while (count--)
    {
        for (int i = 0; i < vrun.size(); i++)
        {
            if (vrun[i].op == 1) //update, skip
                continue;
            else
            {
                degraded_all++;
                gettimeofday(&begin, 0);
                cout << "Degraded reads [" << i << "](" << vrun[i].key << ")" << endl;

                //full DRAM
                auto iter = object_index.find(vrun[i].key);

                unsigned int sid = 0, offset = 0;
                if (iter != object_index.end())
                {
                    cout << "Encoded" << endl;
                    sid = (iter->second).first;
                    offset = (iter->second).second;
                }
                else
                {
                    cout << "Not encoded" << endl;
                    continue;
                }

                cout << "\tsid= " << sid << " offset= " << offset << endl;

                int err_arr[2] = {-1, -1};
                err_arr[0] = offset; //itself
                if (offset == 0)
                {
                    err_arr[1] = 1;
                }
                else
                {
                    err_arr[1] = offset - 1; //assump front & itself are failed;
                }

                cout << "Assume " << err_arr[0] << " and " << err_arr[1] << " fail\n";

                gettimeofday(&bb, 0);
                //DRAM
                char *left_data[K];
                int in = 0;
                for (int j = 0; j < K; j++)
                {
                    if (j == err_arr[0] || j == err_arr[1])
                        continue;
                    left_data[in++] = memcached_get(memc[0], stripe_index[sid][j].first.c_str(), stripe_index[sid][j].first.length(), &val_len, &flags, &rc);
                    if (rc == MEMCACHED_SUCCESS)
                        cout << "\tRead Success " << stripe_index[sid][j].first.c_str() << endl;
                }
                cout << "Read DRAM DATA OVER!" << endl;

                //XOR parity
                char p1[100] = {0};
                sprintf(p1, "SID%u-P%d", sid, 1);
                left_data[in++] = memcached_get(memc[0], p1, strlen(p1), &val_len, &flags, &rc);
                if (rc == MEMCACHED_SUCCESS)
                    cout << "Read DRAM XOR OVER!" << endl;

                gettimeofday(&ee, 0);
                time_dram_all += timeval_diff(&bb, &ee);

                gettimeofday(&bb, 0);
                //disk
                FILE *fin = fopen("16K.txt", "r");
                char tmp[102400] = {0};

                left_data[in] = new char[CHUNK_SIZE];
                vector<char *> v_left;

                if (fin)
                {
                    fgets(tmp, 102400, fin);
                    for (int j = 0; j < strlen(tmp) / CHUNK_SIZE; j++)
                    {
                        char *t = new char[CHUNK_SIZE];
                        memcpy(t, tmp + CHUNK_SIZE * j, CHUNK_SIZE);
                        v_left.push_back(t);
                    }

                    fclose(fin);
                    cout << "Read Disk P2 OVER! " << endl;
                }
                else
                    cerr << "FILE OPEN FAILED" << endl;

                //XOR, only for two, simulator
                unsigned char encode_gftbl_xor[32 * 2 * 1];
                unsigned char encode_matrix_xor[3 * 2];

                gf_gen_rs_matrix(encode_matrix_xor, 3, 2);
                ec_init_tables(2, 1, &(encode_matrix_xor[2 * 2]), encode_gftbl_xor);

                char *tt[2];
                for (int j = 0; j < 2; j++)
                {
                    tt[j] = new char[CHUNK_SIZE];
                    memcpy(tt[j], v_left[j], CHUNK_SIZE);
                }

                ec_encode_data(CHUNK_SIZE, 2, 1, encode_gftbl_xor, (unsigned char **)tt, (unsigned char **)&(left_data[in]));

                cout << "\tCREATE NEW PARITY OK" << endl;

                gettimeofday(&ee, 0);
                time_disk_all += timeval_diff(&bb, &ee);

                unsigned char encode_matrix[N * K] = {0};
                unsigned char error_matrix[N * K] = {0};
                unsigned char invert_matrix[N * K] = {0};
                unsigned char decode_matrix[N * K] = {0};

                for (int j = 0, r = 0; j < N; j++)
                {
                    if (j == err_arr[0] || j == err_arr[1])
                        continue;
                    for (int a = 0; a < K; a++)
                        error_matrix[r * K + a] = encode_matrix[j * K + a];
                    r++;
                }

                gf_invert_matrix(error_matrix, invert_matrix, K);

                cout << "Error Matrix" << endl;

                for (int e = 0; e < 2; e++)
                {
                    int idx = err_arr[e];
                    if (idx < K) // We lost one of the buffers containing the data
                    {
                        for (int j = 0; j < K; j++)
                            decode_matrix[e * K + j] = invert_matrix[idx * K + j];
                    }
                    else // We lost one of the buffer containing the error correction codes
                    {
                        for (int j = 0; j < K; j++)
                        {
                            unsigned char s = 0;
                            //mul the encode matrix coefficient to get the failed data only
                            for (int a = 0; a < K; a++)
                                s ^= gf_mul(invert_matrix[a * K + j], encode_matrix[idx * K + a]);
                            decode_matrix[e * K + j] = s;
                        }
                    }
                }

                cout << "Decode Matrix" << endl;

                unsigned char decode_gftbl[32 * K * (N - K)];
                unsigned char *recovery_data[2];

                for (int j = 0; j < 2; j++)
                    recovery_data[j] = new unsigned char[CHUNK_SIZE];

                for (int j = 0; j < K; j++)
                {
                    left_data[j] = new char[CHUNK_SIZE];
                    memset(left_data[j], '1', CHUNK_SIZE * sizeof(char));
                }

                ec_init_tables(K, N - K, decode_matrix, decode_gftbl);
                ec_encode_data(CHUNK_SIZE, K, 2, decode_gftbl, (unsigned char **)left_data, recovery_data);

                cout << "Decode OVER" << endl
                     << endl;

                for (int j = 0; j < 2; j++)
                    delete[] recovery_data[j];
                delete left_data[in];

                gettimeofday(&end, 0);
                time_all += timeval_diff(&begin, &end);
            }
        }
    }
}

int main(int argc, char *argv[])
{
    //para config
    N = 14;
    K = 10;
    SERVER = 16;
    CHUNK_SIZE = 4096;

    sscanf(argv[2], "%d", &N);
    sscanf(argv[3], "%d", &K);
    sscanf(argv[4], "%s", IP);

    sscanf(argv[1], "%s", path);
    init();

    run();

    cerr << endl
         << "[PL] ALL Time: " << time_all << " s" << endl;
    cerr << "ALL Degraded: " << degraded_all << endl;
    cerr << "AVG time: " << time_all / degraded_all * 1.0 * 1000000 << "us" << endl;
    cerr << "AVG time_dram_all of K-1 chunks: " << time_dram_all / degraded_all * 1.0 * 1000000 / (K - 1) << "us"  << endl;
    cerr << "AVG time_disk_all of 1 chunks (add transfer time): " << time_disk_all / degraded_all * 1.0 * 1000000 << "us"  << endl;

    time_all = 0;
    time_dram_all = 0;
    time_disk_all = 0;
    degraded_all = 0;

    run_plr();

    cerr << endl
         << "[PLR] ALL Time: " << time_all << " s" << endl;
    cerr << "ALL Degraded: " << degraded_all << endl;
    cerr << "AVG time: " << time_all / degraded_all * 1.0 * 1000000 << "us"  << endl;
    cerr << "AVG time_dram_all of K-1 chunks: " << time_dram_all / degraded_all * 1.0 * 1000000 / (K - 1) << "us"  << endl;
    cerr << "AVG time_disk_all of 1 chunks (add transfer time): " << time_disk_all / degraded_all * 1.0 * 1000000 << "us"  << endl;

    return 0;
}