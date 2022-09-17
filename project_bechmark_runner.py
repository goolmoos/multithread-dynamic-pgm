import itertools
from subprocess import check_output
from tqdm import tqdm
f = open("output_pgm_1_thread.csv", "w+")

f.write(f"thread_num,query_num,iter_type,prefill,alg,insert,query,delete\n")
for thread_num, query_num, choice, prefill, alg in tqdm(list(itertools.product(range(1,9), range(1000, 20000, 1000), ["-s", ""], range(1000, 20000, 1000), ["PGM","wormhole"]))):
	M = f"-M{query_num}" if choice else "-M50000"
	commandline = ["./project-benchmark", "-i1000", "-d1000", "-m0", M,
					 f"-p{prefill}", f"-t{thread_num}", f"-q{query_num}", choice, alg]

	# print(" ".join(commandline))
	out = check_output(commandline).decode()
	# print(out)
	f.write(f"{thread_num},{query_num},{'seq' if choice else 'rand'},{prefill},{alg},{out}")
f.close()
