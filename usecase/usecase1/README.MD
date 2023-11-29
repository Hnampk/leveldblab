#
Backup trên cùng luồng
Mô tả
Bình thường service sẽ thực hiện đọc ghi trên MainDB
Đến thời gian thực hiện backup (Backup time), service sẽ chuyển luồng ghi sang vùng nhớ của TempDB và thực hiện backup MainDB
Khi này dữ liệu sẽ nằm ở cả MainDB và TempDB => cần thực hiện cơ chế đối chiếu state khi truy vấn để tìm ra trạng thái cuối cùng
Sau khi backup xong Main DB, service chuyển lại luồng ghi về MainDB, chuẩn bị thực hiện merge state từ TempDB vào MainDB
Việc merge được thực hiện bằng cách duyệt tất cả state trên TempDB, ghi sang MainDB (đảm bảo không ghi đè) và xóa state trên TempDB
Trong quá trình thực hiện merge (Merge time), vẫn cần đối chiếu state trên cả 02 vùng DB khi truy vấn để tìm ra trạng thái cuối cùng
Sau khi merge xong, chuyển về trạng thái bình thường, thực hiện đọc ghi trên MainDB
