import sys

def parse_temperature_data(data_string):
    """
    Parse temperature data from the format:
    {City1=min/mean/max,City2=min/mean/max,...}
    
    Returns a dictionary with city names as keys and (min, mean, max) tuples as values.
    """
    # Remove curly braces and split by commas
    data_string = data_string.strip().strip('{}')
    
    city_data = {}
    
    if not data_string:
        return city_data
    
    entries = data_string.split(',')
    
    for entry in entries:
        entry = entry.strip()
        if '=' in entry:
            city, temps = entry.split('=', 1)
            city = city.strip()
            
            # Parse temperatures (min/mean/max)
            temp_parts = temps.split('/')
            if len(temp_parts) == 3:
                try:
                    min_temp = float(temp_parts[0])
                    mean_temp = float(temp_parts[1])
                    max_temp = float(temp_parts[2])
                    city_data[city] = (min_temp, mean_temp, max_temp)
                except ValueError:
                    print(f"Warning: Could not parse temperatures for {city}: {temps}")
    
    return city_data

def compare_temperature_data():
    """
    Read temperature data from stdin and compare it with result_good.txt
    """
    # Read input from stdin
    try:
        input_data = sys.stdin.read().strip()
    except Exception as e:
        print(f"Error reading from stdin: {e}")
        return
    
    # Parse input data
    input_temps = parse_temperature_data(input_data)
    
    # Read and parse the reference file
    try:
        with open('result_good.txt', 'r') as f:
            reference_data = f.read().strip()
        reference_temps = parse_temperature_data(reference_data)
    except FileNotFoundError:
        print("Error: result_good.txt file not found")
        return
    except Exception as e:
        print(f"Error reading result_good.txt: {e}")
        return
    
    # Compare the data
    print("Temperature Comparison Results:")
    print("=" * 50)
    
    all_cities = set(input_temps.keys()) | set(reference_temps.keys())
    differences_found = False
    
    for city in sorted(all_cities):
        if city not in input_temps:
            print(f"{city}: Missing in input data")
            differences_found = True
            continue
        
        if city not in reference_temps:
            print(f"{city}: Missing in reference data")
            differences_found = True
            continue
        
        input_min, input_mean, input_max = input_temps[city]
        ref_min, ref_mean, ref_max = reference_temps[city]
        
        # Compare with small tolerance for floating point precision
        tolerance = 1e-6
        
        min_match = abs(input_min - ref_min) < tolerance
        mean_match = abs(input_mean - ref_mean) < tolerance
        max_match = abs(input_max - ref_max) < tolerance
        
        if not (min_match and mean_match and max_match):
            print(f"{city}:")
            print(f"  Input:     {input_min:6.1f}/{input_mean:5.1f}/{input_max:5.1f}")
            print(f"  Reference: {ref_min:6.1f}/{ref_mean:5.1f}/{ref_max:5.1f}")
            
            if not min_match:
                print(f"  Min temp differs by: {input_min - ref_min:+.3f}")
            if not mean_match:
                print(f"  Mean temp differs by: {input_mean - ref_mean:+.3f}")
            if not max_match:
                print(f"  Max temp differs by: {input_max - ref_max:+.3f}")
            print()
            differences_found = True
    
    if not differences_found:
        print("✓ All temperature data matches perfectly!")
    else:
        print(f"Found differences in temperature data.")
    
    # Summary statistics
    print("\nSummary:")
    print(f"Cities in input: {len(input_temps)}")
    print(f"Cities in reference: {len(reference_temps)}")
    print(f"Cities in both: {len(set(input_temps.keys()) & set(reference_temps.keys()))}")

if __name__ == "__main__":
    compare_temperature_data()
