# import threading
from queue import  Queue

class SharedQueue:
    def __init__(self, initial_size: int = 1000):
        self.queue = Queue()
        # self.size = initial_size
        # self.requests = [None] * self.size  # List to hold the requests
        # self.count = 0
        # self.start = 0
        # self.end = 0
        # self.request_queue_mutex = threading.Lock()

    def enqueue(self, request):
        # print("enqueue")
        self.queue.put(request)
        # with self.request_queue_mutex:
        #     if self.count == self.size:
        #         self.enlarge_request_queue()
        #     self.requests[self.end] = request
        #     self.end = (self.end + 1) % self.size
        #     self.count += 1
        #     # print(self.count)

    def dequeue(self):
        if not self.empty():
            return self.queue.get()
        return None
        # with self.request_queue_mutex:
        #     if self.count > 0:
        #         answer = self.requests[self.start]
        #         self.start = (self.start + 1) % self.size
        #         self.count -= 1
        #         return answer
        #     else:
        #         return None

    def empty(self) -> bool:
        return self.queue.empty()
        # with self.request_queue_mutex:
        #     return self.count == 0

    # # Protected methods (used internally)
    # def current_size(self) -> int:
    #     return len(self.queue)
    #     # return self.size
    #
    # def current_start(self) -> int:
    #     return self.start
    #
    # def current_end(self) -> int:
    #     return self.end
    #
    # def current_count(self) -> int:
    #     return self.count
    #
    # # Private method to enlarge the queue
    # def enlarge_request_queue(self):
    #     new_size = self.size * 2
    #     new_queue = [None] * new_size
    #     cursor = self.start
    #     new_cursor = 0
    #     while cursor != self.end:
    #         new_queue[new_cursor] = self.requests[cursor]
    #         new_cursor += 1
    #         cursor = (cursor + 1) % self.size
    #     self.size = new_size
    #     self.start = 0
    #     self.end = new_cursor
    #     self.requests = new_queue
