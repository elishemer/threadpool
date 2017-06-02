/*********************************************************
*
*  Copyright (C) 2014 by Vitaliy Vitsentiy
*
*  Licensed under the Apache License, Version 2.0 (the "License");
*  you may not use this file except in compliance with the License.
*  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing, software
*  distributed under the License is distributed on an "AS IS" BASIS,
*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*  See the License for the specific language governing permissions and
*  limitations under the License.
*
*********************************************************/

#ifndef __ctpl_stl_thread_pool_H__
#define __ctpl_stl_thread_pool_H__

#include <functional>
#include <thread>
#include <atomic>
#include <vector>
#include <memory>
#include <exception>
#include <future>
#include <mutex>
#include <queue>

// thread pool to run user's functors with signature
//      ret func(int id, other_params)
// where id is the index of the thread that runs the functor
// ret is some return type

namespace ctpl {

    namespace detail {
        template <typename T>
        class Queue {
        public:
            Queue(size_t size) : capacity(size+1), front(0), back(0), pushWaiting(0), popWaiting(0) {
                q.resize(capacity);
            }

            bool push(T const & value) {
                std::unique_lock<std::mutex> lock(this->mutex);
                ++pushWaiting;
                size_t newback = (back + 1) % capacity;
                while (newback == front)
                    cv_push.wait(lock);
                q[back]=value;
                back = newback;
                --pushWaiting;
                if (popWaiting)
                	cv_pop.notify_one();
                return true;
            }
            // deletes the retrieved element, do not use for non integral types
            bool pop(T & v) {
                std::unique_lock<std::mutex> lock(this->mutex);
                ++popWaiting;
                while (front == back && !isFlush)
                    cv_pop.wait(lock);
                if (front == back)
                    return false;
                v = q[front];
                front = (front+1) % capacity;
                --popWaiting;
                if (pushWaiting)
                	cv_push.notify_one();
                return true;
            }
            bool try_pop(T & v) {
                std::unique_lock<std::mutex> lock(this->mutex);
                if (front == back)
                    return false;
                v = q[front];
                front = (front+1) % capacity;
                if (pushWaiting)
                    cv_push.notify_one();
                return true;
            }
            bool empty() {
                return front == back;
            }
            void flush() {
                std::unique_lock<std::mutex> lock(this->mutex);
                isFlush=true;
                cv_pop.notify_all();
            }
            void unflush() {
                isFlush=false;
            }
            size_t push_waiting() { return pushWaiting; }
            size_t pop_waiting() { return popWaiting; }
        private:
            std::vector<T> q;
            size_t capacity;
            size_t front;
            size_t back;
            size_t pushWaiting;
            size_t popWaiting;
            std::mutex mutex;
            std::condition_variable cv_pop;
            std::condition_variable cv_push;
            bool isFlush;
        };
    }

    class thread_pool {

    public:

        thread_pool(size_t nThreads, size_t nSize = 0) : q(nSize ? nSize : nThreads) {
            add_threads(nThreads);
        }

        // the destructor waits for all the functions in the queue to be finished
        ~thread_pool() {
            this->stop(true);
        }

        void add_threads(size_t nThreads)
        {
            this->threads.resize(threads.size() + nThreads);
            for (int i = 0; i < nThreads; ++i)
                this->set_thread(i);
        }

        // get the number of running threads in the pool
        int size() { return static_cast<int>(this->threads.size()); }

        int n_idle() { return (int)this->q.pop_waiting(); }

        // empty the queue
        void clear_queue() {
            std::function<void(int id)> * _f;
            while (this->q.try_pop(_f))
                delete _f; // empty the queue
        }

        // wait for all computing threads to finish and stop all threads
        // may be called asynchronously to not pause the calling thread while waiting
        // if isWait == true, all the functions in the queue are run, otherwise the queue is cleared without running the functions
        void stop(bool isWait = false) {
            if (!isWait)
                this->clear_queue();  // empty the queue
            this->q.flush();

            for (int i = 0; i < static_cast<int>(this->threads.size()); ++i) // wait for the computing threads to finish
                    if (this->threads[i]->joinable())
                        this->threads[i]->join();
            // if there were no threads in the pool but some functors in the queue, the functors are not deleted by the threads
            // therefore delete them here
            this->clear_queue();
            this->threads.clear();
            this->q.unflush();
        }

        template<typename F, typename... Rest>
        auto push(F && f, Rest&&... rest) ->std::future<decltype(f(0, rest...))> {
            auto pck = std::make_shared<std::packaged_task<decltype(f(0, rest...))(int)>>(
                std::bind(std::forward<F>(f), std::placeholders::_1, std::forward<Rest>(rest)...)
                );
            auto _f = new std::function<void(int id)>([pck](int id) {
                (*pck)(id);
            });
            this->q.push(_f);
            return pck->get_future();
        }

        // run the user's function that excepts argument int - id of the running thread. returned value is templatized
        // operator returns std::future, where the user can get the result and rethrow the catched exceptins
        template<typename F>
        auto push(F && f) ->std::future<decltype(f(0))> {
            auto pck = std::make_shared<std::packaged_task<decltype(f(0))(int)>>(std::forward<F>(f));
            auto _f = new std::function<void(int id)>([pck](int id) {
                (*pck)(id);
            });
            this->q.push(_f);
            return pck->get_future();
        }

    private:
        // deleted
        thread_pool(const thread_pool &);// = delete;
        thread_pool(thread_pool &&);// = delete;
        thread_pool & operator=(const thread_pool &);// = delete;
        thread_pool & operator=(thread_pool &&);// = delete;

        void set_thread(int i) {
            auto f = [this, i]() {
                std::function<void(int id)> * _f;
                while (this->q.pop(_f)) {
                    std::unique_ptr<std::function<void(int id)>> func(_f); // at return, delete the function even if an exception occurred
                    (*_f)(i);
                }
            };
            this->threads[i].reset(new std::thread(f)); // compiler may not support std::make_unique()
        }

        std::vector<std::unique_ptr<std::thread>> threads;
        detail::Queue<std::function<void(int id)> *> q;
        std::mutex mutex;
    };

}

#endif // __ctpl_stl_thread_pool_H__
