MULTI-LEVEL QUEUE

==================================================================

1. Triển khai
------------------

Để triển khai chính sách Lập lịch Hàng đợi Đa cấp (Multi-Level Queue - MLQ), những thay đổi chính đã được thực hiện trong tệp `multi-level_queue.c`.

a. Mở rộng `policy_t`:
   - Một giá trị mới, `POLICY_MLQ`, đã được thêm vào `enum policy_t` để hệ thống có thể nhận diện và gọi chính sách lập lịch mới.

b. Cập nhật hàm `scheduler_get_next_job(policy_t policy)`

==================================================================

2. CƠ CHẾ HOẠT ĐỘNG
-----------------------------

Chính sách MLQ được triển khai chia các công việc đang chờ thành 3 hàng đợi ảo với các mức ưu tiên khác nhau. Bộ lập lịch luôn ưu tiên chọn công việc từ hàng đợi cao nhất. Chỉ khi hàng đợi cao hơn trống, nó mới xét đến hàng đợi thấp hơn.

a. Hàng đợi 1: Ưu tiên cao (Highest Priority)
   - Điều kiện đưa vào: Các công việc có `priority <= 2`. Đây là những công việc quan trọng, cần phản hồi nhanh.
   - Chính sách lập lịch: **Priority Scheduling**. Các công việc trong hàng đợi này được sắp xếp dựa trên độ ưu tiên (số nhỏ hơn được ưu tiên cao hơn). Nếu có hai công việc cùng độ ưu tiên, công việc nào đến trước (FCFS) sẽ được chọn.

b. Hàng đợi 2: Ưu tiên trung bình (Medium Priority)
   - Điều kiện đưa vào: Các công việc có `priority == 3`. Đây là các công việc xử lý hàng loạt thông thường.
   - Chính sách lập lịch: **Shortest Job First (SJF)**. Bằng cách ưu tiên các công việc có thời gian chạy ngắn nhất, hàng đợi này tối ưu hóa thời gian chờ trung bình và tăng thông lượng.

c. Hàng đợi 3: Ưu tiên thấp (Lowest Priority)
   - Điều kiện đưa vào: Các công việc có `priority >= 4`. Đây là các công việc nền, ít quan trọng (ví dụ: xử lý log, bảo trì).
   - Chính sách lập lịch: **First-Come, First-Served (FCFS)**. Các công việc này được xử lý tuần tự khi hệ thống rảnh rỗi và không còn công việc nào quan trọng hơn.

==================================================================

3. KẾT QUẢ VÀ PHÂN TÍCH
-----------------------------------------

Sau khi chay thu tren file workload_mlq_vs_fifo, chúng ta có thể thấy sự vượt trội rõ rệt của chính sách MLQ so với FIFO trên cùng một workload.

| Chỉ số                  | Chính sách FIFO | Chính sách MLQ | Phân tích                                                              |
|-------------------------|-----------------|----------------|------------------------------------------------------------------------|
| Thời gian chờ TB        | 0.50            | 0.40           | **Cải thiện 20%**. MLQ giảm đáng kể thời gian chờ.                      |
| Thời gian quay vòng TB  | 6.90            | 6.80           | **Cải thiện nhẹ**. Tổng thời gian từ lúc đến lúc xong việc nhanh hơn.    |
| Thông lượng (jobs/time)  | 0.500           | 0.513          | **Cao hơn**. MLQ hoàn thành được nhiều công việc hơn trong một đơn vị thời gian. |
| Hiệu suất Worker (%)    | 80.00%          | 82.05%         | **Cao hơn**. Các worker được giữ bận rộn và làm việc hiệu quả hơn.      |
| Tổng thời gian chạy     | 40              | 39             | **Nhanh hơn**. Toàn bộ workload được xử lý xong sớm hơn 1 đơn vị thời gian. |

**Kết luận:**

Chính sách MLQ tỏ ra hiệu quả hơn hẳn so với FIFO. Bằng cách phân loại công việc và áp dụng các thuật toán lập lịch phù hợp cho từng loại, MLQ đã:
- **Tăng khả năng phản hồi:** Ưu tiên các công việc quan trọng, giúp chúng được thực thi gần như ngay lập tức.
- **Tối ưu hóa tài nguyên:** Giảm thời gian chờ đợi lãng phí và giữ cho các worker hoạt động hiệu quả hơn.

Đối với một hệ thống thực tế với nhiều loại công việc khác nhau, MLQ là một lựa chọn vượt trội để cân bằng giữa hiệu suất, thông lượng và tính công bằng.
