#!/usr/bin/python3

import task as tk

for x in range(0,1):
    t = tk.Task();
    t.random_runtime();
    t.random_input();
    t.estimate_runtime(0.125)

    print(t);
