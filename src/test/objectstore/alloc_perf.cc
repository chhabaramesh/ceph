// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Bitmap based in-memory allocator unit test cases.
 * Author: Ramesh Chander, Ramesh.Chander@sandisk.com
 */
#include "os/bluestore/Allocator.h"
#include "global/global_init.h"
#include <iostream>
#include "include/Context.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/errno.h"
#include "include/stringify.h"
#include <gtest/gtest.h>
#include <os/bluestore/BitAllocator.h>
#include "common/ceph_argparse.h"
#include "os/ObjectStore.h"

#define dout_subsys ceph_subsys_filestore

#if GTEST_HAS_PARAM_TEST

#define bmap_test_assert(x) ASSERT_EQ(true, (x))
#define NUM_THREADS 16
#define MAX_BLOCKS (1024 * 1024 * 1)

inline uint64_t
get_time_usecs(void)
{
	struct timeval tv = { 0, 0};
	gettimeofday(&tv, NULL);
	return ((tv.tv_sec * 1000 * 1000) + tv.tv_usec);
}

int num_threads = 8;
int64_t total_size = (int64_t) 1024 * 1024 * 1024;
int64_t total_time = 300;
bool test_run = true;

int64_t _total_ops = 0;


class AllocPerfTest : public ::testing::TestWithParam<const char*> {
public:
    boost::scoped_ptr<Allocator> alloc;
    AllocPerfTest(): alloc(0) { }
    void init_alloc(int64_t size, uint64_t min_alloc_size) {
      std::cout << "Creating alloc type " << string(GetParam()) << " \n";
      alloc.reset(Allocator::create(string(GetParam()), size, min_alloc_size));
    }

    void init_close() {
      alloc.reset(0);
    }
};

typedef struct worker_args {
//    boost::scoped_ptr<Allocator> alloc;
	Allocator *alloc;
	int64_t total_size;
	int num_iter;
	int64_t block_size;
} worker_args_t;

void pre_alloc_blocks(Allocator *alloc, int64_t size, uint64_t block_size)
{
	int64_t i = 0;
	int group = 64;
	int64_t blocks = size / block_size;
	int64_t bmaps = blocks / group;

	for (i = 0; i < blocks; i += 2) {
		alloc->init_add_free(i * block_size, block_size);
	}
#if 0
	for (i = 0; i < bmaps - 1; i += 2) {
		alloc->init_add_free(i * group * block_size, block_size *  group);
	}
#endif
	return;
}

void *
stats_thread(void *args)
{
	int64_t allocated = 0;
	while (test_run) {
		sleep(1);
		printf("Total ops = %ld.\n", _total_ops - allocated);
		allocated = _total_ops;
	}
	return NULL;
}

void *
workers(void *args)
{
	worker_args_t *w_args = (worker_args_t *) args;
    //boost::scoped_ptr<Allocator> alloc = w_args->alloc;

	Allocator *alloc = w_args->alloc;
	int64_t block_size = w_args->block_size;
//	int64_t total_size = w_args->total_size;
	int64_t total_ops = 0;

	while (test_run) {
		uint64_t off = 0;
		uint32_t length = 0;
		alloc->reserve(block_size);
		int ret = alloc->allocate(block_size, block_size, 0, &off, &length);
		assert(ret == 0);

#if 0
		alloc->release(off, length);
		alloc->commit_start();
		alloc->commit_finish();
#else
		alloc->init_add_free(off, length);
#endif

		ret = 0;
		assert(length == block_size);
		total_ops++;
		if (total_ops % 1000 == 0) {
			__sync_fetch_and_add(&_total_ops, total_ops);
			total_ops = 0;
		}
	}


	return NULL;
}


TEST_P(AllocPerfTest, test_time_tput)
{
	int64_t size = total_size;
	int64_t block_size = 1;
	worker_args_t w_args;
	init_alloc(size, block_size);

	w_args.alloc = alloc.get();
	w_args.block_size = block_size;
	w_args.total_size = size;

	pre_alloc_blocks(w_args.alloc, size, 1);

	/*
	 * Create threads and start work for them.
	 */
	printf("Starting %d thread...\n", num_threads);
	pthread_t thread_id[128];
	int i = 0;
	for (i = 0; i < num_threads; i++) {
		if (pthread_create(&thread_id[i], NULL, workers, (void *) &w_args)!= 0 ) {
		    perror("pthread_create: ");
		    exit(1);
		}
	}
	if (pthread_create(&thread_id[i], NULL, stats_thread, NULL)!= 0 ) {
		perror("pthread_create: ");
		exit(1);
	}

	/*
	 * Sleep for given time and wait for threads.
	 */
	printf("Sleeping for %ld secs.\n", total_time);
	sleep(total_time);
	test_run = false;   //Stop tests

	for(i = 0; i < num_threads + 1; i++) {
		if( pthread_join(thread_id[i], NULL) != 0 ) {
			perror("pthread_join: ");
			exit(1);
		} 
	}

	printf("Total ops = %ld, in %ld secs.\n", _total_ops, total_time);
	test_run = true;
	_total_ops = 0;
}

INSTANTIATE_TEST_CASE_P(
  Allocator,
  AllocPerfTest,
  ::testing::Values("bitmap", "stupid"));

#else

TEST(DummyTest, ValueParameterizedTestsAreNotSupportedOnThisPlatform) {}
#endif

int main(int argc, char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);
  env_to_vec(args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  std::string val;
  vector<const char*>::iterator i = args.begin();
  while (i != args.end()) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
	}

    if (ceph_argparse_witharg(args, i, &val, "--size", (char*)nullptr)) {
		total_size = atoi(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "--threads", (char*)nullptr)) {
		num_threads = atoi(val.c_str());
    } else if (ceph_argparse_witharg(args, i, &val, "--time", (char*)nullptr)) {
		total_time = atoi(val.c_str());
	} else {
		printf("Invalid argument %s.\n", val.c_str());
		exit(0);
	}
  }

  ::testing::InitGoogleTest(&argc, argv);
  int r = RUN_ALL_TESTS();
  g_ceph_context->put();
  return r;
}
