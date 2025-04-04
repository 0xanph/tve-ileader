import streamlit as st
import duckdb
import polars as pl
import pandas as pd
import json

st.set_page_config(
    page_title="Quản lý Đăng ký & Bảng điểm",
    page_icon="📚",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- CSS Customization ---
st.markdown("""
<style>
.st-emotion-cache-1r4qj8v, .st-emotion-cache-16txtl3 { padding: 1rem; border-radius: 10px; }
.status-paid {color: green; font-weight: bold;}
.status-unpaid {color: red; font-weight: bold;}
</style>
""", unsafe_allow_html=True)

@st.cache_resource
def load_data():
    return duckdb.connect().execute("SELECT * FROM 'combined_extract.parquet'").pl()

@st.cache_data
def prepare_students():
    df = load_data()
    students = {}
    for row in df.iter_rows(named=True):
        name = row["Tên"]
        if name not in students:
            students[name] = {
                "info": {
                    "Điện thoại": row.get("Điện thoại", ""),
                    "Ngày sinh": row.get("Ngày sinh", ""),
                    "Ghi chú": row.get("Ghi chú", "")
                },
                "records": []
            }
        students[name]["records"].append({
            "Khóa học": row.get("class_id", ""),
            "Lớp": row.get("Tên Lớp", ""),
            "Từ ngày": row.get("start_date", ""),
            "Đến ngày": row.get("end_date", ""),
            "Đã thanh toán": row.get("Đã thanh toán", ""),
            "grades": row.get("grades", "[]")
        })
    return students

@st.cache_data
def get_class_list():
    conn = duckdb.connect()
    return conn.execute('SELECT DISTINCT "Tên Lớp" FROM "combined_extract.parquet" ORDER BY "Tên Lớp"').fetchdf()["Tên Lớp"].tolist()

def display_grades(grades_json):
    try:
        grades = json.loads(grades_json)
        for grade in grades:
            st.markdown(f"**📌 {grade.get('grade_type', 'Không rõ loại bảng điểm')}**")
            components = grade.get("components", {})
            st.table(pd.DataFrame(components.items(), columns=["Thành phần", "Giá trị"]))
    except json.JSONDecodeError:
        st.error("Dữ liệu bảng điểm không hợp lệ!")

def main():
    students = prepare_students()

    st.title("📚 Quản lý Đăng ký & Bảng điểm")

    menu = st.sidebar.radio("🔍 Chế độ xem", ["Học viên", "Lớp"])

    if menu == "Học viên":
        student_name = st.sidebar.selectbox("Chọn học viên", sorted(students.keys()))
        student = students[student_name]

        st.subheader(f"👤 {student_name}")
        col1, col2, col3 = st.columns(3)
        col1.write(f"**📞 Điện thoại:** {student['info']['Điện thoại']}")
        col2.write(f"**🎂 Ngày sinh:** {student['info']['Ngày sinh']}")
        col3.write(f"**📝 Ghi chú:** {student['info']['Ghi chú']}")

        for record in student["records"]:
            payment_status = record["Đã thanh toán"]
            payment_label = f"<span class='status-paid'>✅ {payment_status}</span>" if payment_status == "Hoàn thành HP" else f"<span class='status-unpaid'>❌ {payment_status}</span>"
            with st.expander(f"📖 {record['Lớp']} ({record['Khóa học']}) - {payment_status}"):
                st.write(f"🗓️ **Từ ngày:** {record['Từ ngày']}  ➡️ **Đến ngày:** {record['Đến ngày']}")
                st.markdown(payment_label, unsafe_allow_html=True)
                display_grades(record["grades"])

    elif menu == "Lớp":
        class_name = st.sidebar.selectbox("Chọn lớp", get_class_list())
        st.subheader(f"🏫 {class_name}")

        for student_name, student in students.items():
            for record in student["records"]:
                if record["Lớp"] == class_name:
                    payment_status = record["Đã thanh toán"]
                    payment_label = f"<span class='status-paid'>✅ {payment_status}</span>" if payment_status == "Hoàn thành HP" else f"<span class='status-unpaid'>❌ {payment_status}</span>"
                    with st.expander(f"👤 {student_name}"):
                        st.write(f"📞 **Điện thoại:** {student['info']['Điện thoại']}")
                        st.write(f"🎂 **Ngày sinh:** {student['info']['Ngày sinh']}")
                        st.write(f"📝 **Ghi chú:** {student['info']['Ghi chú']}")
                        st.write(f"🗓️ **Từ ngày:** {record['Từ ngày']} ➡️ **Đến ngày:** {record['Đến ngày']}")
                        st.markdown(payment_label, unsafe_allow_html=True)
                        display_grades(record["grades"])

if __name__ == "__main__":
    main()
