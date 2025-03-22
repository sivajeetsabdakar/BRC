import mmap
import os
import multiprocessing as mp
from concurrent.futures import ThreadPoolExecutor
import io
import threading

global_lock = threading.Lock()

def process_chunk(args):
    filepath, start, end = args
    
    min_temps = {}
    max_temps = {}
    sum_temps = {}
    counts = {}
    
    with open(filepath, 'r+b') as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            pos = start
            line_start = pos
            
            while pos < end:
                if mm[pos] == ord('\n'):
                    line = mm[line_start:pos]
                    
                    sep_pos = -1
                    for i in range(len(line)-1, -1, -1):
                        if line[i] == ord(';'):
                            sep_pos = i
                            break
                    
                    if sep_pos != -1:
                        city = bytes(line[:sep_pos])
                        try:
                            temp_str = line[sep_pos+1:]
                            temp = float(temp_str)
                            
                            if city in min_temps:
                                if temp < min_temps[city]:
                                    min_temps[city] = temp
                                if temp > max_temps[city]:
                                    max_temps[city] = temp
                                sum_temps[city] += temp
                                counts[city] += 1
                            else:
                                min_temps[city] = temp
                                max_temps[city] = temp
                                sum_temps[city] = temp
                                counts[city] = 1
                                
                        except (ValueError, IndexError):
                            pass
                    
                    line_start = pos + 1
                
                pos += 1
            
            if line_start < end:
                line = mm[line_start:end]
                sep_pos = -1
                for i in range(len(line)-1, -1, -1):
                    if line[i] == ord(';'):
                        sep_pos = i
                        break
                
                if sep_pos != -1:
                    city = bytes(line[:sep_pos])
                    try:
                        temp_str = line[sep_pos+1:]
                        temp = float(temp_str)
                        
                        if city in min_temps:
                            if temp < min_temps[city]:
                                min_temps[city] = temp
                            if temp > max_temps[city]:
                                max_temps[city] = temp
                            sum_temps[city] += temp
                            counts[city] += 1
                        else:
                            min_temps[city] = temp
                            max_temps[city] = temp
                            sum_temps[city] = temp
                            counts[city] = 1
                            
                    except (ValueError, IndexError):
                        pass
    
    return min_temps, max_temps, sum_temps, counts

def find_chunk_boundaries(filepath, start, end):
    with open(filepath, 'r+b') as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            if start > 0:
                while start < end and mm[start-1] != ord('\n'):
                    start += 1
            
            if end < len(mm):
                while end < len(mm) and mm[end] != ord('\n'):
                    end += 1
                if end < len(mm):
                    end += 1
    
    return start, end

def merge_chunk_results(result, all_results):
    min_temps, max_temps, sum_temps, counts = result
    
    with global_lock:
        for city, min_val in min_temps.items():
            if city in all_results[0]:
                if min_val < all_results[0][city]:
                    all_results[0][city] = min_val
                if max_temps[city] > all_results[1][city]:
                    all_results[1][city] = max_temps[city]
                all_results[2][city] += sum_temps[city]
                all_results[3][city] += counts[city]
            else:
                all_results[0][city] = min_val
                all_results[1][city] = max_temps[city]
                all_results[2][city] = sum_temps[city]
                all_results[3][city] = counts[city]

def main():
    filepath = 'testcase.txt'
    
    num_cpu = 4
    num_processes = num_cpu * 2
    file_size = os.path.getsize(filepath)
    chunk_size = max(file_size // num_processes, 4 * 1024 * 1024)
    
    all_results = [{}, {}, {}, {}]
    
    tasks = []
    
    for i in range(0, file_size, chunk_size):
        start = i
        end = min(i + chunk_size, file_size)
        
        start, end = find_chunk_boundaries(filepath, start, end)
        
        if end > start:
            tasks.append((filepath, start, end))
    
    with mp.Pool(processes=num_processes) as pool:
        with ThreadPoolExecutor(max_workers=num_processes) as executor:
            for result in pool.imap_unordered(process_chunk, tasks):
                executor.submit(merge_chunk_results, result, all_results)
    
    min_temps, max_temps, sum_temps, counts = all_results
    cities = list(min_temps.keys())
    cities.sort()
    
    with io.StringIO(newline='') as output:
        for i, city in enumerate(cities):
            city_str = city.decode('ascii') if isinstance(city, bytes) else city
            mean_val = sum_temps[city] / counts[city]
            output.write(f"{city_str}={min_temps[city]:.1f}/{mean_val:.1f}/{max_temps[city]:.1f}")
            
            if i < len(cities) - 1:
                output.write('\n')
        
        output_content = output.getvalue()
    
    with open('output.txt', 'w') as f:
        f.write(output_content)

if __name__ == "__main__":
    main()
