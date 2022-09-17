import pandas as pd
import itertools

df = pd.read_csv('output.csv')




for thread_num, choice, prefill in itertools.product(range(1,9), ["seq", "rand"], range(1000, 20000, 1000)):
    dfil = df[(df.iter_type == choice) & (df.thread_num==thread_num) & (df.iter_type==choice) & (df.prefill==prefill)]
    dfil = dfil[["alg","query_num","query"]]
    dpgm = dfil[df.alg == "PGM"][["query_num","query"]]
    dold = dfil[df.alg == "PGM_old"][["query_num","query"]]
    dwormhole = dfil[df.alg == "wormhole"][["query_num","query"]]
    dpgm = dpgm.rename(columns={"query": "PGM"})
    dold = dold.rename(columns={"query": "PGM_old"})
    dwormhole = dwormhole.rename(columns={"query": "wormhole"})
    
    
    to_save = dpgm.merge(dwormhole,"inner")
    if thread_num == 1:
        to_save = to_save.merge(dold,"inner")
    to_save.to_csv(f"./by_prefill_threads/threads{thread_num}_{choice}_prefill{prefill}.csv", index=False)


for query_num, choice, prefill in itertools.product(range(1000, 20000, 1000), ["seq", "rand"], range(1000, 20000, 1000)):
    dfil = df[(df.iter_type == choice) & (df.query_num==query_num) & (df.iter_type==choice) & (df.prefill==prefill)]
    dfil = dfil[["alg","thread_num","query"]]
    dpgm = dfil[df.alg == "PGM"][["thread_num","query"]]
    dold = dfil[df.alg == "PGM_old"][["thread_num","query"]]
    dwormhole = dfil[df.alg == "wormhole"][["thread_num","query"]]
    dpgm = dpgm.rename(columns={"query": "PGM"})
    dold = dold.rename(columns={"query": "PGM_old"})
    dwormhole = dwormhole.rename(columns={"query": "wormhole"})
    
    
    to_save = dpgm.merge(dwormhole,"inner")
    if thread_num == 1:
        to_save = to_save.merge(dold,"inner")
    to_save.to_csv(f"./by_prefill_queries/queries{query_num}_{choice}_prefill{prefill}.csv", index=False)