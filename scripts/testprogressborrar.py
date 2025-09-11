from progress_tracker import set_progress, get_progress
import time

user_id = 3

for i in range(0, 101, 10):
    set_progress(user_id, i)
    print(f"Escrito: {i}%")
    print("Leído:", get_progress(user_id))
    time.sleep(1)
