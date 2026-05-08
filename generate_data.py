import csv
import random

def generate_dataset(filename, num_records, max_arrival):
    sellers = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']
    job_types = ['resize', 'thumbnail', 'compress', 'watermark', 'transcode', 'crop', 'policy_check']
    
    with open(filename, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['job_id', 'seller_id', 'arrival_time', 'estimated_runtime', 'priority', 'job_type'])
        
        for i in range(1, num_records + 1):
            # Nén khung arrival_time để ép các mốc thời gian phải đè lên nhau (overlap)
            arrival_time = random.randint(0, max_arrival)
            
            # Giữ burst time ở mức tương đương file gốc (1 đến 30)
            estimated_runtime = random.randint(1, 30) 
            priority = random.randint(1, 5)
            
            writer.writerow([
                i, 
                random.choice(sellers), 
                arrival_time, 
                estimated_runtime, 
                priority, 
                random.choice(job_types)
            ])
    print(f"[+] Đã tạo thành công {filename} với {num_records} dòng (max arrival_time = {max_arrival}).")

if __name__ == '__main__':
    # Tạo 1000 dòng: arrival_time từ 0 -> 100 (trung bình 10 job tới cùng 1 thời điểm)
    generate_dataset('jobs_1000.csv', 1000, 100)

    # Tạo 10000 dòng: arrival_time từ 0 -> 500 (trung bình 20 job tới cùng 1 thời điểm)
    generate_dataset('jobs_10000.csv', 10000, 500)