import streamlit as st
import duckdb
import polars as pl
import pandas as pd  # used for timestamp conversion and st.table display
import json

# Set up Streamlit page configuration
st.set_page_config(
    page_title="Hệ thống Quản lý Đăng ký & Bảng điểm",
    page_icon=":bar_chart:",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Custom CSS for improved UI ---
custom_css = """
<style>
/* Table font */
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
def load_data_with_duckdb(query: str = "SELECT * FROM 'combined_extract.parquet'"):
    """
    Uses DuckDB to query the Parquet file and returns a Polars DataFrame.
    We use .to_arrow_table() to convert the DuckDB query result directly to an Arrow table,
    then convert it to a Polars DataFrame.
    """
    try:
        arrow_table = duckdb.query(query).to_arrow_table()
        pl_df = pl.from_arrow(arrow_table)
        return pl_df
    except Exception as e:
        st.error(f"Lỗi khi tải dữ liệu với DuckDB: {e}")
        return pl.DataFrame()

def transform_data(df: pl.DataFrame):
    """
    Transforms the flat Polars DataFrame into a nested structure keyed by student.
    
    Each student (identified by column "Tên") gets:
      - "Họ & Tên": student name.
      - "Điện thoại", "Ngày sinh", "Ghi chú_": personal info.
      - "Lịch sử học": list of enrollment records (sorted most recent first).
    
    Each enrollment record includes:
      "STT", "class_id" (displayed as "Khóa Học"), "Tên Lớp" (displayed as "Lớp"),
      "start_date" ("Từ ngày"), "end_date" ("Đến ngày"),
      "Đã thanh toán", and "grades" (a JSON string containing grade records).
    """
    students = {}
    # Convert Polars DataFrame to a list of dictionaries for iteration.
    rows = df.to_dicts()
    for row in rows:
        student_name = str(row.get("Tên", "")).strip()
        if not student_name:
            continue
        # Get personal info.
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
    for student in students.values():
        student["Lịch sử học"].sort(key=lambda x: x.get("parsed_start") or pd.Timestamp.min, reverse=True)
        for enrollment in student["Lịch sử học"]:
            enrollment.pop("parsed_start", None)
        student.pop("enrollment_keys", None)
    return students

def get_students_by_class(students, selected_class):
    """
    Filters the students to only include those with at least one enrollment record
    where "Lớp" equals the selected_class.
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
    Parses the JSON string from the "grades" field and displays each grade record
    in a nicely formatted way. Each grade record is shown with a header (grade type)
    and a table of its components.
    """
    if not grades_json_str or pd.isnull(grades_json_str):
        st.write("Không có dữ liệu bảng điểm cho đăng ký này.")
        returna
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
    
    # Load data from Parquet via DuckDB.
    df = load_data_with_duckdb("SELECT * FROM 'combined_extract.parquet'")
    if df.is_empty():
        st.stop()
    students = transform_data(df)
    
    st.sidebar.header("Điều hướng & Tìm kiếm")
    view = st.sidebar.radio("Chọn chế độ hiển thị", ("Xem theo Học viên", "Xem theo Lớp"))
    st.sidebar.download_button("📥 Tải dữ liệu gốc", df.to_pandas().to_csv(index=False).encode('utf-8'), "du_lieu_goc.csv", "text/csv")
    
    if view == "Xem theo Học viên":
        st.header("Giao diện Học viên")
        all_students = sorted(list(students.keys()))
        search_term = st.sidebar.text_input("Tìm kiếm học viên", "")
        filtered_student_names = [name for name in all_students if search_term.lower() in name.lower()] if search_term else all_students
        selected_student = st.sidebar.selectbox("Chọn học viên", filtered_student_names)
        student_data = students.get(selected_student, {})
        st.subheader(f"Thông tin Học viên: {selected_student} :bust_in_silhouette:")
        
        col_info1, col_info2, col_info3 = st.columns(3)
        with col_info1:
            st.markdown(f"**Điện thoại:** {student_data.get('Điện thoại', 'Chưa cập nhật')}")
        with col_info2:
            st.markdown(f"**Ngày sinh:** {student_data.get('Ngày sinh', 'Chưa cập nhật')}")
        with col_info3:
            st.markdown(f"**Ghi chú:** {student_data.get('Ghi chú_', 'Không có')}")
        
        enrollments = student_data.get("Lịch sử học", [])
        st.write("### Lịch sử đăng ký (mới nhất trước)")
        if enrollments:
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
        classes = sorted(df["Tên Lớp"].drop_nulls().unique().to_list())
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
