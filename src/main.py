def process_chunk(chunk_data):
    """Process a chunk of data and return city statistics"""
    local_stats = {}
    
    for line in chunk_data.split(b'\n'):
        if not line.strip():
            continue
            
        try:
            line_text = line.decode('utf-8', errors='ignore').strip()
            city, temp_str = line_text.split(';', 1)
            temp = float(temp_str)
            
            if city in local_stats:
                stats = local_stats[city]
                if temp < stats[0]:
                    stats[0] = temp
                if temp > stats[2]:
                    stats[2] = temp
                stats[1] += temp
                stats[3] += 1
            else:
                # [min, sum, max, count]
                local_stats[city] = [temp, temp, temp, 1]
        except (ValueError, IndexError, UnicodeDecodeError):
            continue # badly fomed lines
            
    return local_stats

def main(input_file_name="testcase.txt", output_file_name="output.txt"):
    """

    Input format: city;temperature
    Output format: city=min/mean/max (with 1 decimal place)
    """
    import multiprocessing as mp
    import os
    import mmap
    import sys
    
    try:
        file_size = os.path.getsize(input_file_name)
        # For test files
        if file_size < 1024 * 1024:  # < 1MB
            use_multiprocessing = False
        else:
            use_multiprocessing = True
            
        
        cpu_count = mp.cpu_count()
        # Fewer processes for smaller files
        if file_size < 100 * 1024 * 1024:  # <100MB
            num_processes = max(2, min(4, cpu_count))
        else:
            num_processes = max(4, min(8, cpu_count))
            
    except (OSError, FileNotFoundError):
        print(f"Error accessing file: {input_file_name}", file=sys.stderr)
        return
        
    # Process the file
    if use_multiprocessing:
        # for large files
        try:
            with open(input_file_name, 'r+b') as f:
                # Memory map 
                mm = mmap.mmap(f.fileno(), 0)
                
                # chunk size - aim 
                chunk_size = max(1024 * 1024, file_size // num_processes)  # > 1MB per chunk
                
                
                chunks = []
                start = 0
                
                while start < file_size:
                    end = min(file_size, start + chunk_size)
                    
                    if end < file_size:
                        mm.seek(end)
                        try:
                            mm.readline()
                            end = mm.tell()
                        except ValueError:
                            end = file_size
                    
                    mm.seek(start)
                    chunk = mm.read(end - start)
                    chunks.append(chunk)
                    
                    start = end
                
                # in parallel
                with mp.Pool(processes=num_processes) as pool:
                    chunk_results = pool.map(process_chunk, chunks)
                
                mm.close()
        except (IOError, ValueError, OSError) as e:
            #  if memory mapping fails
            print(f"Memory mapping failed, falling back to standard processing: {e}", file=sys.stderr)
            use_multiprocessing = False
            
    # fallback or for small files
    if not use_multiprocessing:
        try:
            with open(input_file_name, 'rb') as f:
                chunk_results = [process_chunk(f.read())]
        except (IOError, OSError) as e:
            print(f"Error reading file: {e}", file=sys.stderr)
            return
    
    city_stats = {}
    for local_stats in chunk_results:
        for city, stats in local_stats.items():
            min_temp, sum_temp, max_temp, count = stats
            
            if city in city_stats:
                merged_stats = city_stats[city]
                merged_stats[0] = min(merged_stats[0], min_temp) 
                merged_stats[1] += sum_temp 
                merged_stats[2] = max(merged_stats[2], max_temp) 
                merged_stats[3] += count 
            else:
                city_stats[city] = stats.copy()
    
    output_lines = []
    for city in sorted(city_stats.keys()):
        min_temp, sum_temps, max_temp, count = city_stats[city]
        mean_temp = sum_temps / count
        
        formatted_result = f"{city}={min_temp:.1f}/{mean_temp:.1f}/{max_temp:.1f}"
        output_lines.append(formatted_result)
    
    try:
        with open(output_file_name, 'w') as output_file:
            output_file.write('\n'.join(output_lines))
    except (IOError, OSError) as e:
        print(f"Error writing output file: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()