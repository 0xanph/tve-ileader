import streamlit as st
import pandas as pd
import json
from datetime import datetime

# Cấu hình trang
st.set_page_config(
    page_title="Dashboard Đăng ký & Bảng điểm",
    page_icon=":bar_chart:",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- CSS tùy chỉnh cho giao diện đẹp ---
custom_css = """
<style>
/* Căn chỉnh font cho bảng */
table {
    font-size: 14px;
}
.st-expanderContent {
    background-color: #f0f2f6;
    padding: 1rem;
    border-radius: 5px;
}
.enrollment-title {
    font-size: 16px;
    font-weight: bold;
    color: #34495e;
    margin-bottom: 0.5rem;
}
.grade-header {
    font-size: 18px;
    font-weight: bold;
    color: #2c3e50;
    margin-bottom: 0.5rem;
}
.status-paid {
    color: green;
    font-weight: bold;
}
.status-unpaid {
    color: red;
    font-weight: bold;
}
.student-info {
    font-size: 15px;
    margin-bottom: 0.5rem;
}
</style>
"""
st.markdown(custom_css, unsafe_allow_html=True)

@st.cache_data
def load_and_combine_data():
    """
    Reads the combined Parquet file and drops duplicate enrollment records
    based on the key columns: "Tên", "class_id", "Tên Lớp", "start_date", "end_date".
    """
    try:
        df = pd.read_parquet("combined_extract.parquet", engine="pyarrow")
    except Exception as e:
        st.error(f"Lỗi khi tải file Parquet: {e}")
        return pd.DataFrame()
    dedup_columns = ["Tên", "class_id", "Tên Lớp", "start_date", "end_date"]
    df = df.drop_duplicates(subset=dedup_columns)
    return df

def transform_data(df):
    """
    Chuyển đổi dữ liệu phẳng thành cấu trúc lồng nhau theo học viên.
    
    Mỗi học viên (được nhận diện bởi cột "Tên") sẽ có:
      - "Họ & Tên": tên học viên.
      - "Điện thoại", "Ngày sinh", "Ghi chú_": thông tin cá nhân.
      - "Lịch sử học": danh sách các đăng ký (sắp xếp theo thứ tự mới nhất).
    
    Mỗi đăng ký bao gồm các trường:
      "STT", "class_id" (hiển thị là "Khóa Học"),
      "Tên Lớp" (hiển thị là "Lớp"),
      "start_date" (hiển thị là "Từ ngày"),
      "end_date" (hiển thị là "Đến ngày"),
      "Đã thanh toán", và "grades" (chuỗi JSON chứa bảng điểm).
    """
    students = {}
    for _, row in df.iterrows():
        student_name = str(row.get("Tên", "")).strip()
        if not student_name:
            continue
        # Lấy thông tin cá nhân của học viên
        phone = str(row.get("Điện thoại", "")).strip()
        birth = str(row.get("Ngày sinh", "")).strip()
        note = str(row.get("Ghi chú_", "")).strip()
        
        if student_name not in students:
            students[student_name] = {
                "Họ & Tên": student_name,
                "Điện thoại": phone,
                "Ngày sinh": birth,
                "Ghi chú_": note,
                "Lịch sử học": [],
                "enrollment_keys": set()
            }
        
        enrollment_key = (
            str(row.get("class_id", "")),
            str(row.get("Tên Lớp", "")),
            str(row.get("start_date", "")),
            str(row.get("end_date", ""))
        )
        if enrollment_key in students[student_name]["enrollment_keys"]:
            continue
        students[student_name]["enrollment_keys"].add(enrollment_key)
        
        grades_str = row.get("grades", "")
        try:
            parsed_start = pd.to_datetime(row.get("start_date", ""), dayfirst=True, errors="coerce")
        except Exception:
            parsed_start = None
        
        enrollment = {
            "STT": row.get("STT"),
            "Khóa Học": str(row.get("class_id", "")),
            "Lớp": row.get("Tên Lớp", ""),
            "Từ ngày": row.get("start_date", ""),
            "Đến ngày": row.get("end_date", ""),
            "Đã thanh toán": str(row.get("Đã thanh toán", "")).strip(),
            "grades": grades_str,
            "parsed_start": parsed_start
        }
        students[student_name]["Lịch sử học"].append(enrollment)
    
    # Sắp xếp lịch sử đăng ký theo thứ tự mới nhất trước
    for student in students.values():
        student["Lịch sử học"].sort(key=lambda x: x.get("parsed_start") or pd.Timestamp.min, reverse=True)
        for enrollment in student["Lịch sử học"]:
            enrollment.pop("parsed_start", None)
        student.pop("enrollment_keys", None)
    return students

def get_students_by_class(students, selected_class):
    """
    Lọc các học viên có ít nhất một đăng ký với "Lớp" bằng selected_class.
    """
    filtered_students = {}
    for student_name, student_data in students.items():
        enrollments = student_data.get("Lịch sử học", [])
        filtered_enrollments = [enroll for enroll in enrollments if enroll.get("Lớp", "") == selected_class]
        if filtered_enrollments:
            student_copy = {
                "Họ & Tên": student_data["Họ & Tên"],
                "Điện thoại": student_data.get("Điện thoại", ""),
                "Ngày sinh": student_data.get("Ngày sinh", ""),
                "Ghi chú_": student_data.get("Ghi chú_", ""),
                "Lịch sử học": filtered_enrollments
            }
            filtered_students[student_name] = student_copy
    return filtered_students

def display_grade_records(grades_json_str):
    """
    Phân tích chuỗi JSON từ cột "grades" và hiển thị từng bảng điểm đẹp mắt.
    Mỗi bảng điểm được hiển thị với tiêu đề (loại bảng điểm) và bảng chi tiết thành phần.
    """
    if not grades_json_str or pd.isnull(grades_json_str):
        st.write("Không có dữ liệu bảng điểm cho đăng ký này.")
        return

    try:
        grade_records = json.loads(grades_json_str)
    except Exception as e:
        st.write("Lỗi phân tích bảng điểm:", e)
        st.write(grades_json_str)
        return

    if not grade_records:
        st.write("Không tìm thấy bảng điểm nào.")
        return

    for record in grade_records:
        grade_type = record.get("grade_type", "Loại bảng điểm không xác định")
        st.markdown(f"<div class='grade-header'>📊 {grade_type}</div>", unsafe_allow_html=True)
        components = record.get("components", {})
        if components:
            df_components = pd.DataFrame(list(components.items()), columns=["Thành phần", "Giá trị"])
            st.table(df_components)
        else:
            st.write("Không có thành phần bảng điểm.")

def main():
    st.title("Hệ thống Quản lý Đăng ký & Bảng điểm")
    
    df = load_and_combine_data()
    if df.empty:
        st.stop()
    students = transform_data(df)
    
    # Sidebar: Điều hướng, tìm kiếm và lọc.
    st.sidebar.header("Điều hướng & Tìm kiếm")
    view = st.sidebar.radio("Chọn chế độ hiển thị", ("Xem theo Học viên", "Xem theo Lớp"))
    st.sidebar.download_button("📥 Tải dữ liệu gốc", df.to_csv(index=False).encode('utf-8'), "du_lieu_goc.csv", "text/csv")
    
    if view == "Xem theo Học viên":
        st.header("Giao diện Học viên")
        all_students = sorted(list(students.keys()))
        search_term = st.sidebar.text_input("Tìm kiếm học viên", "")
        if search_term:
            filtered_student_names = [name for name in all_students if search_term.lower() in name.lower()]
        else:
            filtered_student_names = all_students
        selected_student = st.sidebar.selectbox("Chọn học viên", filtered_student_names)
        student_data = students.get(selected_student, {})
        # Hiển thị thông tin cá nhân của học viên.
        st.subheader(f"Thông tin Học viên: {selected_student} :bust_in_silhouette:")
        col_info1, col_info2, col_info3 = st.columns(3)
        with col_info1:
            st.markdown(f"**Điện thoại:** {student_data.get('Điện thoại', 'Chưa cập nhật')}")
        with col_info2:
            st.markdown(f"**Ngày sinh:** {student_data.get('Ngày sinh', 'Chưa cập nhật')}")
        with col_info3:
            st.markdown(f"**Ghi chú:** {student_data.get('Ghi chú_', 'Không có')}")
        
        enrollments = student_data.get("Lịch sử học", [])
        if enrollments:
            st.write("### Lịch sử đăng ký (mới nhất trước)")
            for enrollment in enrollments:
                col1, col2 = st.columns([1, 2])
                with col1:
                    st.markdown(f"<div class='enrollment-title'>📚 Mã khóa học: {enrollment.get('Khóa Học','')} | Lớp: {enrollment.get('Lớp','')}</div>", unsafe_allow_html=True)
                    st.write(f"**Từ ngày:** {enrollment.get('Từ ngày','')}")
                    st.write(f"**Đến ngày:** {enrollment.get('Đến ngày','')}")
                    status = enrollment.get("Đã thanh toán", "")
                    if status == "Hoàn thành HP":
                        st.markdown(f"**Đã thanh toán:** <span class='status-paid'>✅ {status}</span>", unsafe_allow_html=True)
                    else:
                        st.markdown(f"**Đã thanh toán:** <span class='status-unpaid'>❌ {status}</span>", unsafe_allow_html=True)
                with col2:
                    with st.expander("Xem bảng điểm"):
                        display_grade_records(enrollment.get("grades", ""))
                st.markdown("---")
        else:
            st.write("Không tìm thấy đăng ký nào cho học viên này.")
    
    elif view == "Xem theo Lớp":
        st.header("Giao diện Lớp học")
        classes = sorted(df["Tên Lớp"].dropna().unique().tolist())
        selected_class = st.sidebar.selectbox("Chọn Lớp", classes)
        search_student = st.sidebar.text_input("Tìm kiếm theo tên học viên trong lớp", "")
        filtered_students = get_students_by_class(students, selected_class)
        if search_student:
            filtered_students = {name: data for name, data in filtered_students.items() if search_student.lower() in name.lower()}
        st.subheader(f"Lớp: {selected_class} :school:")
        st.write("### Danh sách Học viên trong lớp:")
        if filtered_students:
            for student_name, student_data in filtered_students.items():
                st.markdown(f"**👤 {student_name}**")
                st.markdown(f"**Điện thoại:** {student_data.get('Điện thoại', 'Chưa cập nhật')}, **Ngày sinh:** {student_data.get('Ngày sinh', 'Chưa cập nhật')}, **Ghi chú:** {student_data.get('Ghi chú_', 'Không có')}")
                for enrollment in student_data.get("Lịch sử học", []):
                    col1, col2 = st.columns([1, 2])
                    with col1:
                        st.markdown(f"<div class='enrollment-title'>📚 Mã khóa học: {enrollment.get('Khóa Học','')}</div>", unsafe_allow_html=True)
                        st.write(f"**Từ ngày:** {enrollment.get('Từ ngày','')}")
                        st.write(f"**Đến ngày:** {enrollment.get('Đến ngày','')}")
                        status = enrollment.get("Đã thanh toán", "")
                        if status == "Hoàn thành HP":
                            st.markdown(f"**Đã thanh toán:** <span class='status-paid'>✅ {status}</span>", unsafe_allow_html=True)
                        else:
                            st.markdown(f"**Đã thanh toán:** <span class='status-unpaid'>❌ {status}</span>", unsafe_allow_html=True)
                    with col2:
                        with st.expander("Xem bảng điểm"):
                            display_grade_records(enrollment.get("grades", ""))
                    st.markdown("---")
        else:
            st.write("Không tìm thấy học viên nào trong lớp này.")

if __name__ == "__main__":
    main()
