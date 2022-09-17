/*
 * This example shows how to use pgm::DynamicPGMIndex, a std::map-like container supporting inserts and deletes.
 * Compile with:
 *   g++ updates.cpp -std=c++17 -I../include -o updates
 * Run with:
 *   ./updates
 */
#include <vector>
#include <cstdlib>
#include <iostream>
#include <algorithm>
#include <string>
#include <iostream>
#include <unistd.h>
#include <stdio.h>
#include <fcntl.h>
#include <random>
#include <chrono>
#include <thread>

#include "wormhole/lib.h"
#include "wormhole/kv.h"
#include "wormhole/wh.h"

#include "pgm/pgm_index_dynamic.hpp"

#define DEFAULT_PREFILL (100000000)
#define DEFAULT_MAXSIZE (RAND_MAX)

using timer = std::chrono::system_clock;


void print_help() {
    std::cout << "Hola! this is a bench for PGM!" << std::endl;
    std::cout << std::endl;

    std::cout << "Input: PGM for PGM, wormhole for Wormhole" << std::endl;
    std::cout << std::endl;
    
    std::cout << "Arguments:" << std::endl;
    std::cout << "\t-h is help - display this message" << std::endl;
    std::cout << "\t-v is verbose" << std::endl;
    std::cout << "\t-s performs a sequential test on the range of numbers from minValue to maxValue" << std::endl;
    std::cout << "\t\tthis renders i,d,q as irrelevant" << std::endl;
    std::cout << "\t-t[integer] is num of threads" << std::endl;
    std::cout << "\t-m[integer] is mininal int value to randomise" << std::endl;
    std::cout << "\t-M[integer] is maximal int value to randomize" << std::endl;
    std::cout << "\t-p[integer] is number of random insertions to to perform DURING PREFILL" << std::endl;
    std::cout << "\t-i[integer] is number of insertions to perform" << std::endl;
    std::cout << "\t-d[integer] is number of deletions to perform" << std::endl;
    std::cout << "\t-q[integer] is number of queries to perform" << std::endl;
    std::cout << std::endl;
    
    std::cout << "The test does:" << std::endl;
    std::cout << "\t1. prefill" << std::endl;
    std::cout << "\t2. insert" << std::endl;
    std::cout << "\t3. query" << std::endl;
    std::cout << "\t4. delete" << std::endl;

}


void pgm_rand_insert_thread(pgm::DynamicPGMIndex<uint32_t, uint32_t>* dynamic_pgm, int numbers[], int amount) {
    for (int i = 0; i < amount; i++) {
        dynamic_pgm->insert_or_assign(numbers[i],i);
    }
}
void pgm_rand_query_thread(pgm::DynamicPGMIndex<uint32_t, uint32_t>* dynamic_pgm, int numbers[], int amount) {
    for (int i = 0; i < amount; i++) {
        dynamic_pgm->find(numbers[i]);
    }
}
void pgm_rand_delete_thread(pgm::DynamicPGMIndex<uint32_t, uint32_t>* dynamic_pgm, int numbers[], int amount) {
    for (int i = 0; i < amount; i++) {
        dynamic_pgm->erase(numbers[i]);
    }
}

void pgm_seq_insert_thread(pgm::DynamicPGMIndex<uint32_t, uint32_t>* dynamic_pgm, int bot, int top) {
    for (int i = bot; i < top; i++) {
        dynamic_pgm->insert_or_assign(i,i);
    }
}

void pgm_seq_query_thread(pgm::DynamicPGMIndex<uint32_t, uint32_t>* dynamic_pgm, int bot, int top) {
    for (int i = bot; i < top; i++) {
        dynamic_pgm->find(i);
    }
}

void pgm_seq_delete_thread(pgm::DynamicPGMIndex<uint32_t, uint32_t>* dynamic_pgm, int bot, int top) {
    for (int i = bot; i < top; i++) {
        dynamic_pgm->erase(i);
    }
}


int pgm_rand_test(pgm::DynamicPGMIndex<uint32_t, uint32_t>* dynamic_pgm, int minSize, int maxSize, int insertions, int deletions, int queries, int num_of_threads) {


    // INIT
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distr(minSize, maxSize);

    std::vector<std::thread> threads;
    
    int *to_insert =(int*) malloc(insertions*sizeof(int));
    int *to_query =(int*) malloc(queries*sizeof(int));
    int *to_delete =(int*) malloc(deletions*sizeof(int));
    
    if (to_insert == NULL || to_query == NULL  || to_delete == NULL ) {
        exit(-1);
    }
    
    for (int i = 0; i < insertions; i++) {
        to_insert[i] = distr(gen);
    }
    
    for (int i = 0; i < queries; i++) {
        to_query[i] = distr(gen);
    }
    
    for (int i = 0; i < deletions; i++) {
        to_delete[i] = distr(gen);
    }


    // INSERT
    auto t_before_insert = timer::now();
    const auto writer_threads = 1;
    for (int i = 0; i < writer_threads; i++) {
        threads.push_back(std::thread(pgm_rand_insert_thread, dynamic_pgm, to_insert+i*(insertions/writer_threads),insertions/writer_threads));
    }

    for (auto &th : threads) {
        th.join();
    }
    auto t_after_insert = timer::now();
    
    threads.clear();
    
    // QUERY
    auto t_before_query = timer::now();
    for (int i = 0; i < num_of_threads; i++) {
        threads.push_back(std::thread(pgm_rand_query_thread, dynamic_pgm, to_query+i*(queries/num_of_threads),queries/num_of_threads));
    }

    for (auto &th : threads) {
        th.join();
    }
    auto t_after_query = timer::now();
    
    threads.clear();
    
    // DELETE
    auto t_before_delete = timer::now();
    for (int i = 0; i < writer_threads; i++) {
        threads.push_back(std::thread(pgm_rand_delete_thread, dynamic_pgm, to_delete+i*(deletions/writer_threads),deletions/writer_threads));
    }

    for (auto &th : threads) {
        th.join();
    }

    threads.clear();

    auto t_after_delete = timer::now();
    
    
    std::chrono::duration<double> insert_diff = t_after_insert - t_before_insert;
    std::chrono::duration<double> delete_diff = t_after_delete - t_before_delete;
    std::chrono::duration<double> query_diff = t_after_query - t_before_query;
    std::cout << insert_diff.count() << "," << query_diff.count() << "," << delete_diff.count() << std::endl;

    free(to_insert);
    free(to_query);
    free(to_delete);
    return 0; 
}

int pgm_seq_test(pgm::DynamicPGMIndex<uint32_t, uint32_t>* dynamic_pgm, int minSize, int maxSize, int num_of_threads) {

    std::vector<std::thread> threads;

    // INSERT
    auto t_before_insert = timer::now();
    auto writer_threads = 1;
    
    for (int i = 0; i < writer_threads; i++) {
        threads.push_back(std::thread(pgm_seq_insert_thread, dynamic_pgm, minSize + i*((maxSize-minSize)/writer_threads),minSize + (i+1)*((maxSize-minSize)/writer_threads)));
    }

    for (auto &th : threads) {
        th.join();
    }
    auto t_after_insert = timer::now();
    
    threads.clear();



    // QUERY
    auto t_before_query = timer::now();
    for (int i = 0; i < num_of_threads; i++) {
        threads.push_back(std::thread(pgm_seq_query_thread, dynamic_pgm, minSize + i*((maxSize-minSize)/num_of_threads),minSize + (i+1)*((maxSize-minSize)/num_of_threads)));
    }

    for (auto &th : threads) {
        th.join();
    }
    auto t_after_query = timer::now();
    
    threads.clear();


    // DELETE
    auto t_before_delete = timer::now();
    for (int i = 0; i < writer_threads; i++) {
        threads.push_back(std::thread(pgm_seq_delete_thread, dynamic_pgm, minSize + i*((maxSize-minSize)/writer_threads),minSize + (i+1)*((maxSize-minSize)/writer_threads)));
    }

    for (auto &th : threads) {
        th.join();
    }
    auto t_after_delete = timer::now();
    
    threads.clear();

    
    std::chrono::duration<double> insert_diff = t_after_insert - t_before_insert;
    std::chrono::duration<double> delete_diff = t_after_delete - t_before_delete;
    std::chrono::duration<double> query_diff = t_after_query - t_before_query;
    std::cout << insert_diff.count() << "," << query_diff.count() << "," << delete_diff.count() << std::endl;

    return 0; 
}

void wh_rand_insert_thread(struct wormhole * wh, int numbers[], int amount) {
    struct wormref *ref = wh_ref(wh);
    for (int i = 0; i < amount; i++) {
        uint32_t key = __builtin_bswap32(numbers[i]);
        wh_put(ref, &key, 4, &i, 4);
    }
    wh_unref(ref);
}
void wh_rand_query_thread(struct wormhole * wh, int numbers[], int amount) {
    struct wormref *ref = wh_ref(wh);
    struct wormhole_iter * iter = wh_iter_create(ref);
    for (int i = 0; i < amount; i++) {
        uint32_t key = __builtin_bswap32(numbers[i]);
        wh_iter_seek(iter, &key, 4);
        u32 key_out, len_out;
        wh_iter_peek(iter, &key_out, 4, &len_out, NULL, 0, NULL);
    }
    wh_iter_destroy(iter);
    wh_unref(ref);
}
void wh_rand_delete_thread(struct wormhole * wh, int numbers[], int amount) {
    struct wormref *ref = wh_ref(wh);
    for (int i = 0; i < amount; i++) {
        uint32_t key = __builtin_bswap32(numbers[i]);
        wh_del(ref, &key, 4);
    }
    wh_unref(ref);
}

void wh_seq_insert_thread(struct wormhole * wh, int bot, int top) {
    struct wormref *ref = wh_ref(wh);
    for (int i = bot; i < top; i++) {
        uint32_t key = __builtin_bswap32(i);
        wh_put(ref, &key, 4, &i, 4);
    }
    wh_unref(ref);
}

void wh_seq_query_thread(struct wormhole * wh, int bot, int top) {
    struct wormref *ref = wh_ref(wh);
    struct wormhole_iter * iter = wh_iter_create(ref);
    uint32_t bot_key = __builtin_bswap32(bot);
    wh_iter_seek(iter, &bot_key, 4); // seek to first
    for (int i = bot + 1; i < top; i++) {
        wh_iter_skip1(iter);
        u32 key_out, len_out;
        wh_iter_peek(iter, &key_out, 4, &len_out, NULL, 0, NULL);
    }
    wh_iter_destroy(iter);
    wh_unref(ref);
}

void wh_seq_delete_thread(struct wormhole * wh, int bot, int top) {
    struct wormref *ref = wh_ref(wh);
    for (int i = bot; i < top; i++) {
        uint32_t key = __builtin_bswap32(i);
        wh_del(ref, &key, 4);
    }
    wh_unref(ref);
}


int wh_rand_test(struct wormhole * wh, int minSize, int maxSize, int insertions, int deletions, int queries, int num_of_threads) {


    // INIT
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distr(minSize, maxSize);

    std::vector<std::thread> threads;

    int *to_insert =(int*) malloc(insertions*sizeof(int));
    int *to_query =(int*) malloc(queries*sizeof(int));
    int *to_delete =(int*) malloc(deletions*sizeof(int));

    if (to_insert == NULL || to_query == NULL  || to_delete == NULL ) {
        exit(-1);
    }

    for (int i = 0; i < insertions; i++) {
        to_insert[i] = distr(gen);
    }

    for (int i = 0; i < queries; i++) {
        to_query[i] = distr(gen);
    }

    for (int i = 0; i < deletions; i++) {
        to_delete[i] = distr(gen);
    }


    // INSERT
    auto t_before_insert = timer::now();
    for (int i = 0; i < num_of_threads; i++) {
        threads.push_back(std::thread(wh_rand_insert_thread, wh, to_insert+i*(insertions/num_of_threads),insertions/num_of_threads));
    }

    for (auto &th : threads) {
        th.join();
    }
    auto t_after_insert = timer::now();
    threads.clear();

    // QUERY
    auto t_before_query = timer::now();
    for (int i = 0; i < num_of_threads; i++) {
        threads.push_back(std::thread(wh_rand_query_thread, wh, to_query+i*(queries/num_of_threads),queries/num_of_threads));
    }

    for (auto &th : threads) {
        th.join();
    }
    auto t_after_query = timer::now();
    threads.clear();

    // DELETE
    auto t_before_delete = timer::now();
    for (int i = 0; i < num_of_threads; i++) {
        threads.push_back(std::thread(wh_rand_delete_thread, wh, to_delete+i*(deletions/num_of_threads),deletions/num_of_threads));
    }

    for (auto &th : threads) {
        th.join();
    }

    auto t_after_delete = timer::now();


    std::chrono::duration<double> insert_diff = t_after_insert - t_before_insert;
    std::chrono::duration<double> delete_diff = t_after_delete - t_before_delete;
    std::chrono::duration<double> query_diff = t_after_query - t_before_query;
    std::cout << insert_diff.count() << "," << query_diff.count() << "," << delete_diff.count() << std::endl;

    free(to_insert);
    free(to_query);
    free(to_delete);
    return 0;
}

int wh_seq_test(struct wormhole * wh, int minSize, int maxSize, int num_of_threads) {

    std::vector<std::thread> threads;

    // INSERT
    auto t_before_insert = timer::now();
    for (int i = 0; i < num_of_threads; i++) {
        threads.push_back(std::thread(wh_seq_insert_thread, wh, minSize + i*((maxSize-minSize)/num_of_threads),minSize + (i+1)*((maxSize-minSize)/num_of_threads)));
    }

    for (auto &th : threads) {
        th.join();
    }
    auto t_after_insert = timer::now();

    for (int i = 0; i < num_of_threads; i++) {
        threads.pop_back();
    }



    // QUERY
    auto t_before_query = timer::now();
    for (int i = 0; i < num_of_threads; i++) {
        threads.push_back(std::thread(wh_seq_query_thread, wh, minSize + i*((maxSize-minSize)/num_of_threads),minSize + (i+1)*((maxSize-minSize)/num_of_threads)));
    }

    for (auto &th : threads) {
        th.join();
    }
    auto t_after_query = timer::now();

    for (int i = 0; i < num_of_threads; i++) {
        threads.pop_back();
    }


    // DELETE
    auto t_before_delete = timer::now();
    for (int i = 0; i < num_of_threads; i++) {
        threads.push_back(std::thread(wh_seq_delete_thread, wh, minSize + i*((maxSize-minSize)/num_of_threads),minSize + (i+1)*((maxSize-minSize)/num_of_threads)));
    }

    for (auto &th : threads) {
        th.join();
    }
    auto t_after_delete = timer::now();

    for (int i = 0; i < num_of_threads; i++) {
        threads.pop_back();
    }


    std::chrono::duration<double> insert_diff = t_after_insert - t_before_insert;
    std::chrono::duration<double> delete_diff = t_after_delete - t_before_delete;
    std::chrono::duration<double> query_diff = t_after_query - t_before_query;
    std::cout << insert_diff.count() << "," << query_diff.count() << "," << delete_diff.count() << std::endl;

    return 0;
}



int main(int argc, char** argv) {
    
    // options
    std::string input = "";
    int opt;
    bool flagVerbose = false;
    bool flagSequential = false;
    int minSize = 0;
    int maxSize = 0;
    int insertions = 0;
    int deletions = 0;
    int queries = 0;
    int num_of_threads = 1;
    int tries = 1;
    int prefill = DEFAULT_PREFILL;

    // last value is argument
    if ( (argc <= 1) || (argv[argc-1] == NULL) || (argv[argc-1][0] == '-') ) {  // there is NO input...
        std::cerr << "No argument provided!" << std::endl;
        //return 1;
    }
    else {  // there is an input...
        input = argv[argc-1];
    }

    opterr = 0;

    // Retrieve the options:
    while ( (opt = getopt(argc, argv, "hv:s:m:M:i::d::q::p::t::T::")) != -1 ) {  // for each option...
        switch ( opt ) {
            case 'h':
                    print_help();
                    exit(0);
                    break;
            case 'v':
                    flagVerbose = true;
                    break;
            case 's':
                    flagSequential = true;
                    break;
            case 'm':
                    minSize = atoi(optarg);
                    break;
            case 't':
                    num_of_threads = atoi(optarg);
                    break;
            case 'M':
                    maxSize = atoi(optarg);
                    break;
            case 'i':
                    insertions = atoi(optarg);
                    break;
            case 'd':
                    deletions = atoi(optarg);
                    break;
            case 'q':
                    queries = atoi(optarg);
                    break;
            case 'p':
                    prefill = atoi(optarg);
                    break;
            case 'T':
                    tries = atoi(optarg);
                    break;

            case '?':  // unknown option...
                    std::cerr << "Unknown option: '" << char(optopt) << "'!" << std::endl;
                break;
        }
    }


    if (flagVerbose) {
        std::cout << "Starting test with:" << std::endl;
        std::cout << "\tverbose: " << flagVerbose << std::endl;
        std::cout << "\tminsize: " << minSize << std::endl; 
        std::cout << "\tmaxsize: " << maxSize << std::endl; 
        std::cout << "\tinsertions: " << insertions << std::endl; 
        std::cout << "\tdeletions: " << deletions << std::endl; 
        std::cout << "\tqueries: " << queries << std::endl; 
        std::cout << "\tthreads: " << num_of_threads << std::endl;
        std::cout << "\tsequential: " << flagSequential << std::endl;
        std::cout << "\tprefill: " << prefill << std::endl;
    }


    if (input == "PGM") {
        if (flagVerbose) std::cout << "\ttesting for PGM" << std::endl;
    } else if (input == "wormhole") {
        if (flagVerbose) std::cout << "\ttesting for wormhole!" << std::endl;
    } else {
        std::cout << "Invalid params" << std::endl;
        exit(1);
    }

    if (flagVerbose) std::cout << "starting pre-fill" << std::endl;

    // PREFILL
    // random filling data - insertion times
//    std::cout << "INSERT,QUERY,DELETE" << std::endl;

    for (int i = 0; i < tries; i++) {
        std::vector<std::pair<uint32_t, uint32_t>> data(prefill);
        std::generate(data.begin(), data.end(), [minSize,maxSize] {
            // random in range - seeding
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<> distr(minSize, maxSize);
            return std::make_pair(distr(gen),distr(gen)); });
        std::sort(data.begin(), data.end());
        
        if (flagVerbose) std::cout << "pre-fill ended" << std::endl;

        if (input == "PGM")
        {
            // Construct and bulk-load the Dynamic PGM-index
            pgm::DynamicPGMIndex<uint32_t, uint32_t> dynamic_pgm(data.begin(), data.end());
            if (!flagSequential) {
                pgm_rand_test(&dynamic_pgm, minSize,maxSize,insertions,deletions,queries,num_of_threads);
            } else {
                pgm_seq_test(&dynamic_pgm, minSize,maxSize,num_of_threads);
            }
        } else {
            struct wormhole * wh = wh_create();
            struct wormref * ref = wh_ref(wh);
            for (const auto& [first, sec] : data)
            {
                uint32_t key = __builtin_bswap32(first);
                wh_put(ref, &key, 4, &sec, 4);
            }
            wh_unref(ref);
            if (!flagSequential) {
                wh_rand_test(wh, minSize,maxSize,insertions,deletions,queries,num_of_threads);
            } else {
                wh_seq_test(wh, minSize,maxSize,num_of_threads);
            }
            wh_destroy(wh);

        }
    }

    return 0;
}
