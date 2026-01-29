create or replace package "XXXPEN_ESTIMATE_PKG" as

PROCEDURE submit_estimate (estimate_record in xxpen_hr_estimate_overrides_tbl%ROWTYPE,p_process_id out xxpen_hr_estimate_overrides_tbl.process_id%type);

PROCEDURE PROJECTED_EARN_HOURS_ESTIMATE(p_person_number in varchar2,p_process_id in number,p_person_id in XXPEN_HR_PENSION_PARTICIPANTS_TBL.person_id%TYPE,p_ppt in VARCHAR2,p_start_date in date);

PROCEDURE GET_OPTIONAL_FORM_PAGINATION(p_pagination in varchar2);

PROCEDURE CREATE_PAYMENT(p_payment_info_rec in XXPEN_HR_PAYMENT_INFO_TBL%ROWTYPE,p_created_by in varchar2,p_pension_start_date in date,p_payment_frequency in varchar2);

FUNCTION GET_NON_TAXABLE_AMT(p_process_id in number,p_drop_monthly_flag in varchar2,p_drop_selected in varchar2,p_optional_form in varchar2) return varchar2;

TYPE estimate_pension_participants_rt IS RECORD (
    pension_participants_id NUMBER,
    person_id               NUMBER,
    person_number           VARCHAR2(50),
    annual_base_pay	        NUMBER,
    employment_status	    VARCHAR2(50),
    effective_date          DATE
);
TYPE estimate_pension_participants_t IS TABLE OF estimate_pension_participants_rt;

PROCEDURE BULK_ESTIMATE(
    p_pension_start_date IN date,
    p_earnings_assumptions IN varchar2,
    p_assume_salary_increase_per_year IN NUMBER,
    p_person_id IN varchar2 default null,
    p_employment_status IN varchar2 default null,
    p_time_frame IN number default null
);
PROCEDURE SCHEDULE_BULK_ESTIMATE(
    p_pension_start_date IN date,
    p_earnings_assumptions IN varchar2,
    p_assume_salary_increase_per_year IN NUMBER,
    p_person_id IN varchar2 default null,
    p_employment_status IN varchar2 default null,
    p_time_frame IN number default null
);
PROCEDURE SAVE_SERVICE_ESTIMATE_DETAILS(p_person_id in number,p_process_id in number);
PROCEDURE RERUN_PREVIOUS_ESTIMATE(p_process_id in number,p_new_process_id out number,p_calc_name out varchar2);
FUNCTION GET_OPTIONAL_FORM_SELECTED(p_optional_form in varchar2 ,p_drop_election in varchar2) return varchar2;
PROCEDURE get_erd_urd_dates(p_process_id in number, p_pension_plan_type in varchar2,o_erd_gg out date,o_urd_gg out date,o_erd_pf out date,o_urd_pf out date);
end "XXXPEN_ESTIMATE_PKG";
/