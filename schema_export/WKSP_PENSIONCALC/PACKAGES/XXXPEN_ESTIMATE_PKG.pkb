create or replace PACKAGE BODY "XXXPEN_ESTIMATE_PKG"
AS
--jbenegasi test
procedure get_erd_urd_dates(p_process_id in number, p_pension_plan_type in varchar2,o_erd_gg out date,o_urd_gg out date,o_erd_pf out date,o_urd_pf out date) as
begin
if p_pension_plan_type = 'Split Service Pension' then
    select EARLY_RETIREMENT_DATE,NORMAL_RETIREMENT_DATE,ERD_DATE_SPLIT_CAT3_PF,URD_DATE_SPLIT_CAT3_PF
    into o_erd_gg,o_urd_gg,o_erd_pf,o_urd_pf
    from XXPEN_HR_SERVICE_TIME_HEADERS_ESTIMATE_TBL where process_id = p_process_id and pension_plan_type = 'Split Service Pension';
else
   begin
    select EARLY_RETIREMENT_DATE,NORMAL_RETIREMENT_DATE
    into o_erd_pf,o_urd_pf
    from XXPEN_HR_SERVICE_TIME_HEADERS_ESTIMATE_TBL where process_id = p_process_id and pension_plan_type = 'PF';
    exception when no_data_found then
    o_erd_pf := null;
    o_urd_pf := null;
   end;
   begin
    select distinct EARLY_RETIREMENT_DATE,NORMAL_RETIREMENT_DATE
    into o_erd_gg,o_urd_gg
    from XXPEN_HR_SERVICE_TIME_HEADERS_ESTIMATE_TBL where process_id = p_process_id and pension_plan_type in ('GG','MNPS Support');
   exception when no_data_found then
    o_erd_gg := null;
    o_urd_gg := null;
   end;
end if;
END;
   PROCEDURE submit_estimate (
      estimate_record   IN     xxpen_hr_estimate_overrides_tbl%ROWTYPE,
      p_process_id         OUT xxpen_hr_estimate_overrides_tbl.process_id%TYPE)
   IS
      l_estimate_record          xxpen_hr_estimate_overrides_tbl%ROWTYPE
                                    := estimate_record;
      l_pension_plan             xxpen_hr_plan_benefit_info_tbl.pension_plan%TYPE;
      l_pension_plan_type        xxpen_hr_plan_benefit_info_tbl.pension_plan_type%TYPE;
      l_ss_amount                NUMBER;
      l_ss_start_date            DATE;
      l_ss_start_age             NUMBER;
      l_plan_start_date          DATE;
      l_function_name            VARCHAR2 (250);
      l_death_calculation_type   xxpen_hr_plan_calc_attr_tbl.death_calc_type%TYPE;
   BEGIN
      /*insert into xxafw_process_monitor_tbl(process_id,process_type,process_name,person_id,calculation_name,person_number,run_status,calculation_source)
      values(estimate_record.process_id,'Calculation','Pension Calculation',estimate_record.person_id,estimate_record.calculation_name,estimate_record.person_number,'RUNNING','Pension Estimates');
      */
      BEGIN
         SELECT death_calc_type
           INTO l_death_calculation_type
           FROM xxpen_hr_plan_calc_attr_tbl
          WHERE     person_id = estimate_record.person_id
                AND effective_date =
                       (SELECT MAX (effective_date)
                          FROM xxpen_hr_plan_calc_attr_tbl
                         WHERE person_id = estimate_record.person_id)
                AND ROWNUM = 1;
      EXCEPTION
         WHEN NO_DATA_FOUND
         THEN
            NULL;
      END;

      SELECT DISTINCT pension_plan, pension_plan_type, plan_start_date
        INTO l_pension_plan, l_pension_plan_type, l_plan_start_date
        FROM xxpen_hr_plan_benefit_info_tbl
       WHERE     effective_date =
                    (SELECT MAX (effective_date)
                       FROM xxpen_hr_plan_benefit_info_tbl
                      WHERE person_id = estimate_record.person_id)
             AND person_id = estimate_record.person_id;

      xxafw_process_monitor_pkg.init_new_pension_calculation_monitor (
         p_calculation_source        => 'Pension Estimates',
         p_calculation_name          => estimate_record.calculation_name,
         p_pension_plan              => l_pension_plan,
         p_person_id                 => estimate_record.person_id,
         p_person_number             => estimate_record.person_number,
         p_calculation_description   => estimate_record.calculation_description);

      l_estimate_record.process_id :=
         xxafw_process_monitor_pkg.g_monitor_process_id;
      l_estimate_record.override_id := xxpen_hr_estimate_overrides_seq.NEXTVAL;
      p_process_id := l_estimate_record.process_id;

      INSERT INTO xxpen_hr_estimate_overrides_tbl
           VALUES l_estimate_record;

      projected_earn_hours_estimate (
         estimate_record.person_number,
         xxafw_process_monitor_pkg.g_monitor_process_id,
         estimate_record.person_id,
         l_pension_plan_type,
         l_estimate_record.pension_start_date);

      xxpen_pension_calculations_pkg.calc_entire_consolidate_pensionable_earnings (
         p_person_id    => estimate_record.person_id,
         p_process_id   => l_estimate_record.process_id);

      xxpen_pension_calculations_pkg.calc_service_totals (
         p_person_id    => estimate_record.person_id,
         p_process_id   => l_estimate_record.process_id);

      xxpen_pension_calculations_pkg.calc_pension_eligibility (
         p_person_id    => estimate_record.person_id,
         p_process_id   => l_estimate_record.process_id);

      xxpen_pension_calculations_pkg.calc_pension_vesting (
         p_person_id    => estimate_record.person_id,
         p_process_id   => l_estimate_record.process_id);

      xxpen_pension_calculations_pkg.calc_service_points (
         p_person_id => estimate_record.person_id);
      xxpen_pension_calculations_pkg.calc_retirement_dates (
         p_person_id    => estimate_record.person_id,
         p_process_id   => l_estimate_record.process_id);

      xxpen_pension_calculations_pkg.calc_fae_five_years (
         p_person_id    => estimate_record.person_id,
         p_process_id   => l_estimate_record.process_id);        -- 5 YEAR FAE
      xxpen_pension_calculations_pkg.calc_fae_last_year (
         p_person_id    => estimate_record.person_id,
         p_process_id   => l_estimate_record.process_id);        -- 1 YEAR FAE

      IF l_pension_plan = 'Division A'
      THEN
         IF l_pension_plan_type = 'PF'
         THEN
            xxpen_pension_calculations_pkg.calc_unreduced_val_pf_diva (
               p_person_id      => estimate_record.person_id,
               p_ss_start_age   => estimate_record.option_c_start_age,
               p_ss_amount      => estimate_record.option_c_amount,
               p_process_id     => l_estimate_record.process_id);

            xxpen_pension_calculations_pkg.calc_nf_pf_diva (
               p_person_id    => estimate_record.person_id,
               p_process_id   => l_estimate_record.process_id);
         ELSE
            xxpen_pension_calculations_pkg.calc_unreduced_val_diva (
               p_person_id    => estimate_record.person_id,
               p_ss_start_age   => estimate_record.option_c_start_age,
               p_ss_amount      => estimate_record.option_c_amount,
               p_process_id   => l_estimate_record.process_id);

            xxpen_pension_calculations_pkg.calc_nf_gg_mnps_diva (
               p_person_id    => estimate_record.person_id,
               p_process_id   => l_estimate_record.process_id);
         END IF;
      ELSE
         xxpen_pension_calculations_pkg.calc_unreduced_val (
            p_person_id    => estimate_record.person_id,
            p_process_id   => l_estimate_record.process_id);

         --END IF;
         IF l_pension_plan_type = 'PF'
         THEN
            xxpen_pension_calculations_pkg.calc_nf_pf (
               p_person_id    => estimate_record.person_id,
               p_process_id   => l_estimate_record.process_id);
         ELSE
            xxpen_pension_calculations_pkg.calc_nf_gg_mnps (
               p_person_id    => estimate_record.person_id,
               p_process_id   => l_estimate_record.process_id);
         END IF;
      END IF;

      xxpen_pension_calculations_pkg.calc_drop_lump (
         estimate_record.person_id,
         p_process_id        => l_estimate_record.process_id,
         --p_death_calc_type   => 'N');
         p_death_calc_type   => NVL(l_death_calculation_type,'N'));

      xxpen_pension_calculations_pkg.calc_epv (
         p_person_id    => estimate_record.person_id,
         p_process_id   => l_estimate_record.process_id);

      xxpen_pension_calculations_pkg.calc_drop_normal_form (
         p_person_id    => estimate_record.person_id,
         p_process_id   => l_estimate_record.process_id);

      xxpen_pension_calculations_pkg.calc_normal_form_drop1x (
         p_person_id    => estimate_record.person_id,
         p_process_id   => l_estimate_record.process_id);

      xxpen_pension_calculations_pkg.calc_normal_form_drop2x (
         p_person_id    => estimate_record.person_id,
         p_process_id   => l_estimate_record.process_id);

      xxpen_pension_calculations_pkg.calc_normal_form_drop3x (
         p_person_id    => estimate_record.person_id,
         p_process_id   => l_estimate_record.process_id);

      IF l_death_calculation_type IS NOT NULL AND l_death_calculation_type not in('DEATH - SERVICE PENSIONER','DEATH - BENEFICIARY')
      THEN
         -- calc_death code calls calc_option_a code. Hence we are skipping "calc_option_a"
         xxpen_pension_calculations_pkg.calc_death (
            p_person_id         => estimate_record.person_id,
            p_process_id        => l_estimate_record.process_id,
            p_death_calc_type   => l_death_calculation_type);
      ELSE
         xxpen_pension_calculations_pkg.calc_option_a (
            p_person_id    => estimate_record.person_id,
            p_process_id   => l_estimate_record.process_id);
      END IF;

      xxpen_pension_calculations_pkg.calc_option_b (
         p_person_id    => estimate_record.person_id,
         p_process_id   => l_estimate_record.process_id);

      xxpen_pension_calculations_pkg.calc_option_c (
         p_person_id      => estimate_record.person_id,
         p_process_id     => l_estimate_record.process_id,
         p_ss_start_age   => estimate_record.option_c_start_age,
         p_ss_amount      => estimate_record.option_c_amount);

      xxpen_pension_calculations_pkg.calc_option_d (
         p_person_id    => estimate_record.person_id,
         p_process_id   => l_estimate_record.process_id);

      xxpen_pension_calculations_pkg.calc_option_e (
         p_person_id    => estimate_record.person_id,
         p_process_id   => l_estimate_record.process_id);

      xxpen_pension_calculations_pkg.calc_option_f (
         p_person_id    => estimate_record.person_id,
         p_process_id   => l_estimate_record.process_id);

      xxpen_pension_calculations_pkg.calc_option_c_drop1x (
         p_person_id    => estimate_record.person_id,
         p_ss_amount    => estimate_record.option_c_amount,
         p_process_id   => l_estimate_record.process_id);

      xxpen_pension_calculations_pkg.calc_option_c_drop2x (
         p_person_id    => estimate_record.person_id,
         p_ss_amount    => estimate_record.option_c_amount,
         p_process_id   => l_estimate_record.process_id);

      xxpen_pension_calculations_pkg.calc_option_c_drop3x (
         p_person_id    => estimate_record.person_id,
         p_ss_amount    => estimate_record.option_c_amount,
         p_process_id   => l_estimate_record.process_id);

      /* Get FUNCTION NAME OLD 4/30
      IF l_estimate_record.optional_form_selected like '%OPTION%A%' THEN
          l_function_name := 'Option A';
      ELSIF l_estimate_record.optional_form_selected like '%OPTION%B%' THEN
           l_function_name := 'Option B';
      ELSIF l_estimate_record.optional_form_selected like '%OPTION%C%' THEN
           l_function_name := 'Option C';
      ELSIF l_estimate_record.optional_form_selected like '%OPTION%D%' THEN
           l_function_name := 'Option D';
      ELSIF l_estimate_record.optional_form_selected like '%OPTION%E%' THEN
           l_function_name := 'Option E';
      ELSIF l_estimate_record.optional_form_selected like '%OPTION%F%' THEN
           l_function_name := 'Option F';
      ELSE
           l_function_name := 'Normal Form';
      END IF;
      */
      IF UPPER (l_estimate_record.optional_form_selected) LIKE '%ALL%'
      THEN
         l_function_name := 'Normal Form';
      --ELSIF UPPER (l_estimate_record.optional_form_selected) LIKE '%NORMAL%'
      --THEN
     --    l_function_name := 'Normal Form';
      ELSIF UPPER (l_estimate_record.optional_form_selected) LIKE
               '%OPTION%A%'
      THEN
         IF l_estimate_record.drop_election LIKE '%1%'
         THEN
            l_function_name := 'Option A DROP (1x)';
         ELSIF l_estimate_record.drop_election LIKE '%2%'
         THEN
            l_function_name := 'Option A DROP (2x)';
         ELSIF l_estimate_record.drop_election LIKE '%3%'
         THEN
            l_function_name := 'Option A DROP (3x)';
         ELSE
            l_function_name := 'Option A';
         END IF;
      ELSIF UPPER (l_estimate_record.optional_form_selected) LIKE
               '%OPTION%B%'
      THEN
         IF l_estimate_record.drop_election LIKE '%1%'
         THEN
            l_function_name := 'Option B DROP (1x)';
         ELSIF l_estimate_record.drop_election LIKE '%2%'
         THEN
            l_function_name := 'Option B DROP (2x)';
         ELSIF l_estimate_record.drop_election LIKE '%3%'
         THEN
            l_function_name := 'Option B DROP (3x)';
         ELSE
            l_function_name := 'Option B';
         END IF;
      ELSIF UPPER (l_estimate_record.optional_form_selected) LIKE
               '%OPTION%C%'
      THEN
         IF l_estimate_record.drop_election LIKE '%1%'
         THEN
            l_function_name := 'Option C DROP (1x)';
         ELSIF l_estimate_record.drop_election LIKE '%2%'
         THEN
            l_function_name := 'Option C DROP (2x)';
         ELSIF l_estimate_record.drop_election LIKE '%3%'
         THEN
            l_function_name := 'Option C DROP (3x)';
         ELSE
            l_function_name := 'Option C';
         END IF;
      ELSIF UPPER (l_estimate_record.optional_form_selected) LIKE
               '%OPTION%D%'
      THEN
         IF l_estimate_record.drop_election LIKE '%1%'
         THEN
            l_function_name := 'Option D DROP (1x)';
         ELSIF l_estimate_record.drop_election LIKE '%2%'
         THEN
            l_function_name := 'Option D DROP (2x)';
         ELSIF l_estimate_record.drop_election LIKE '%3%'
         THEN
            l_function_name := 'Option D DROP (3x)';
         ELSE
            l_function_name := 'Option D';
         END IF;
      ELSIF UPPER (l_estimate_record.optional_form_selected) LIKE
               '%OPTION%E%'
      THEN
         IF l_estimate_record.drop_election LIKE '%1%'
         THEN
            l_function_name := 'Option E DROP (1x)';
         ELSIF l_estimate_record.drop_election LIKE '%2%'
         THEN
            l_function_name := 'Option E DROP (2x)';
         ELSIF l_estimate_record.drop_election LIKE '%3%'
         THEN
            l_function_name := 'Option E DROP (3x)';
         ELSE
            l_function_name := 'Option E';
         END IF;
      ELSIF UPPER (l_estimate_record.optional_form_selected) LIKE
               '%OPTION%F%'
      THEN
         IF l_estimate_record.drop_election LIKE '%1%'
         THEN
            l_function_name := 'Option F DROP (1x)';
         ELSIF l_estimate_record.drop_election LIKE '%2%'
         THEN
            l_function_name := 'Option F DROP (2x)';
         ELSIF l_estimate_record.drop_election LIKE '%3%'
         THEN
            l_function_name := 'Option F DROP (3x)';
         ELSE
            l_function_name := 'Option F';
         END IF;
      ELSE
         IF l_estimate_record.drop_election LIKE '%1%'
         THEN
            l_function_name := 'Normal Form DROP (1x)';
         ELSIF l_estimate_record.drop_election LIKE '%2%'
         THEN
            l_function_name := 'Normal Form DROP (2x)';
         ELSIF l_estimate_record.drop_election LIKE '%3%'
         THEN
            l_function_name := 'Normal Form DROP (3x)';
         ELSE
            l_function_name := 'Normal Form';
         END IF;
      END IF;

      --XXPEN_PENSION_CALCULATIONS_PKG.CALC_PENSION_PRORATION(p_person_id => estimate_record.person_id,p_process_id => l_estimate_record.process_id,p_calc_func_name => l_function_name);
      /*XXPEN_PENSION_CALCULATIONS_PKG.calc_non_tax (
            p_person_id    => estimate_record.person_id,
            p_process_id   => l_estimate_record.process_id);*/

      xxpen_pension_calculations_pkg.calc_option_d_drop1x (
         p_person_id    => estimate_record.person_id,
         p_process_id   => l_estimate_record.process_id);

      xxpen_pension_calculations_pkg.calc_option_d_drop2x (
         p_person_id    => estimate_record.person_id,
         p_process_id   => l_estimate_record.process_id);

      xxpen_pension_calculations_pkg.calc_option_d_drop3x (
         p_person_id    => estimate_record.person_id,
         p_process_id   => l_estimate_record.process_id);

      xxpen_pension_calculations_pkg.calc_option_a_drop1x (
         p_person_id    => estimate_record.person_id,
         p_process_id   => l_estimate_record.process_id);

      xxpen_pension_calculations_pkg.calc_option_a_drop2x (
         p_person_id    => estimate_record.person_id,
         p_process_id   => l_estimate_record.process_id);

      xxpen_pension_calculations_pkg.calc_option_a_drop3x (
         p_person_id    => estimate_record.person_id,
         p_process_id   => l_estimate_record.process_id);

      xxpen_pension_calculations_pkg.calc_option_b_drop1x (
         p_person_id    => estimate_record.person_id,
         p_process_id   => l_estimate_record.process_id);

      xxpen_pension_calculations_pkg.calc_option_b_drop2x (
         p_person_id    => estimate_record.person_id,
         p_process_id   => l_estimate_record.process_id);

      xxpen_pension_calculations_pkg.calc_option_b_drop3x (
         p_person_id    => estimate_record.person_id,
         p_process_id   => l_estimate_record.process_id);

      xxpen_pension_calculations_pkg.calc_option_e_drop1x (
         p_person_id    => estimate_record.person_id,
         p_process_id   => l_estimate_record.process_id);

      xxpen_pension_calculations_pkg.calc_option_e_drop2x (
         p_person_id    => estimate_record.person_id,
         p_process_id   => l_estimate_record.process_id);

      xxpen_pension_calculations_pkg.calc_option_e_drop3x (
         p_person_id    => estimate_record.person_id,
         p_process_id   => l_estimate_record.process_id);

      xxpen_pension_calculations_pkg.calc_option_f_drop1x (
         p_person_id    => estimate_record.person_id,
         p_process_id   => l_estimate_record.process_id);

      xxpen_pension_calculations_pkg.calc_option_f_drop2x (
         p_person_id    => estimate_record.person_id,
         p_process_id   => l_estimate_record.process_id);

      xxpen_pension_calculations_pkg.calc_option_f_drop3x (
         p_person_id    => estimate_record.person_id,
         p_process_id   => l_estimate_record.process_id);

      xxpen_pension_calculations_pkg.calc_non_tax (
         p_person_id    => estimate_record.person_id,
         p_process_id   => l_estimate_record.process_id);

      xxpen_pension_calculations_pkg.calc_disability_person (
         p_person_id    => estimate_record.person_id,
         p_process_id   => l_estimate_record.process_id);
IF upper(l_estimate_record.EVENT_REASON) like '%DISABILITY%' THEN
      xxpen_pension_calculations_pkg.calc_pension_proration (
         p_person_id        => estimate_record.person_id,
         p_process_id       => l_estimate_record.process_id,
         p_calc_func_name   => 'Disability Pension');
ELSE
      xxpen_pension_calculations_pkg.calc_pension_proration (
         p_person_id        => estimate_record.person_id,
         p_process_id       => l_estimate_record.process_id,
         p_calc_func_name   => l_function_name);
END IF;


      /* xxpen_pension_calculations_pkg.calc_death (
         p_person_id    => estimate_record.person_id,
         p_process_id   => l_estimate_record.process_id,
         p_death_calc_type => l_death_calculation_type ); */

      xxafw_process_monitor_pkg.stop_monitor;
      SAVE_SERVICE_ESTIMATE_DETAILS(estimate_record.person_id,l_estimate_record.process_id);
   END submit_estimate;

   PROCEDURE projected_earn_hours_estimate (
      p_person_number   IN VARCHAR2,
      p_process_id      IN NUMBER,
      p_person_id       IN xxpen_hr_pension_participants_tbl.person_id%TYPE,
      p_ppt                VARCHAR2,
      p_start_date      IN DATE)
   IS
      l_estimate_overrides_rec    xxpen_hr_estimate_overrides_tbl%ROWTYPE;
      l_pension_participants_id   xxpen_hr_pension_participants_tbl.pension_participants_id%TYPE;
      l_person_id                 xxpen_hr_pension_participants_tbl.person_id%TYPE
         := p_person_id;
      l_pay_frequency             xxpen_hr_payment_info_tbl.pay_frequency%TYPE
         := 'M';
      start_date                  NUMBER;
      end_date                    NUMBER;
      l_previous_year             VARCHAR2 (10);
      l_pay_rate                  NUMBER;
      l_hours_per_year            NUMBER := 2080;

      CURSOR c_monthly_pay (
         end_date DATE)
      IS
             SELECT TRUNC (ADD_MONTHS (SYSDATE, LEVEL - 1), 'MM') AS start_day,
                    LAST_DAY (TRUNC (ADD_MONTHS (SYSDATE, LEVEL - 1), 'MM'))
                       AS end_day
               FROM DUAL
         CONNECT BY ADD_MONTHS (SYSDATE, LEVEL - 1) <= end_date;

      CURSOR c_semi_monthly_pay (
         end_date DATE)
      IS
             SELECT TRUNC (ADD_MONTHS (SYSDATE, LEVEL - 1), 'MM') + 14
                       AS start_day,
                    LAST_DAY (TRUNC (ADD_MONTHS (SYSDATE, LEVEL - 1), 'MM'))
                       AS end_day
               FROM DUAL
         CONNECT BY ADD_MONTHS (SYSDATE, LEVEL - 1) <= end_date
         UNION
             SELECT TRUNC (ADD_MONTHS (SYSDATE, LEVEL - 1), 'MM') AS start_day,
                    TRUNC (ADD_MONTHS (SYSDATE, LEVEL - 1), 'MM') + 13 AS end_day
               FROM DUAL
         CONNECT BY ADD_MONTHS (SYSDATE, LEVEL - 1) <= end_date;
   BEGIN
      SELECT *
        INTO l_estimate_overrides_rec
        FROM xxpen_hr_estimate_overrides_tbl
       WHERE process_id = p_process_id;

      /*
          select pension_participants_id
          into l_pension_participants_id
          from XXPEN_HR_PENSION_PARTICIPANTS_TBL
          where person_number = p_person_number
          and effective_date = (select max(effective_date) from XXPEN_HR_PENSION_PARTICIPANTS_TBL where person_number = p_person_number)
          group by pension_participants_id;
       */
      /*
       select pay_frequency
       into l_pay_frequency
       from XXPEN_HR_PAYMENT_INFO_TBL
       where person_id = p_person_id;
   */
      l_pay_rate :=
         NVL (l_estimate_overrides_rec.assume_earnings_per_year,
              l_estimate_overrides_rec.compensation_rate_annual_amt);
      xxafw_process_monitor_pkg.log_info (
            '***STARTING PROJECTED_EARN_HOURS_ESTIMATE PAY FREQUENCY IS: '
         || l_pay_frequency
         || 'PAY RATE IS: '
         || l_pay_rate);

      IF     l_pay_frequency = 'S'
         AND nvl(l_estimate_overrides_rec.termination_date,l_estimate_overrides_rec.EVENT_DATE) >= trunc(SYSDATE) AND l_estimate_overrides_rec.assume_earnings_per_year is not null
      THEN
         FOR r_pay_period
            IN c_semi_monthly_pay (
                  nvl(l_estimate_overrides_rec.termination_date,l_estimate_overrides_rec.EVENT_DATE))
         LOOP
            IF l_previous_year <> TO_CHAR (r_pay_period.end_day, 'YYYY')
            THEN
               l_pay_rate :=
                  ROUND (
                       l_pay_rate
                     * (  1
                        + (  NVL (
                                l_estimate_overrides_rec.assume_salary_increase_per_year,
                                0)
                           / 100)),
                     2);
            END IF;

            INSERT
              INTO xxpen_hr_projected_earn_hours_estimate_tbl (
                      process_id,
                      person_id,
                      earnings,
                      hours,
                      check_date,
                      data_source,
                      pension_plan_type,
                      non_taxable_earnings,
                      pay_frequency,
                      processed_flag,
                      pay_period_start_date,
                      pay_period_end_date)
            VALUES (p_process_id,
                    l_person_id,
                    (l_pay_rate / 26),
                    80,
                    r_pay_period.end_day,
                    'Estimate',
                    p_ppt,
                    'NONTAX TEMP',
                    l_pay_frequency,
                    'T',
                    r_pay_period.start_day,
                    r_pay_period.end_day);

            /*
            insert into XXPEN_HR_SERVICE_TIME_LINES_PROJECTED_TBL (process_id,person_id,projected,adjusted_hours,adjusted_earnings,actual_hours,actual_earnings,service_credit,pay_period_start_date,pay_period_end_Date)
            values(p_process_id,l_person_id,'Y',80,(l_pay_rate/26),80,(l_pay_rate/26),'1',r_pay_period.start_day,r_pay_period.end_day); */
            l_previous_year := TO_CHAR (r_pay_period.end_day, 'YYYY');
         END LOOP;
      ELSIF     l_pay_frequency = 'M'
            AND nvl(l_estimate_overrides_rec.termination_date,l_estimate_overrides_rec.EVENT_DATE) >= trunc(SYSDATE) AND l_estimate_overrides_rec.assume_earnings_per_year is not null
      THEN
         FOR r_pay_period
            IN c_monthly_pay (nvl(l_estimate_overrides_rec.termination_date,l_estimate_overrides_rec.EVENT_DATE))
         LOOP
            IF l_previous_year <> TO_CHAR (r_pay_period.end_day, 'YYYY')
            THEN
               l_pay_rate :=
                  ROUND (
                       l_pay_rate
                     * (  1
                        + (  NVL (
                                l_estimate_overrides_rec.assume_salary_increase_per_year,
                                0)
                           / 100)),
                     2);
            END IF;

            INSERT
              INTO xxpen_hr_projected_earn_hours_estimate_tbl (
                      process_id,
                      person_id,
                      earnings,
                      hours,
                      check_date,
                      data_source,
                      pension_plan_type,
                      non_taxable_earnings,
                      pay_frequency,
                      processed_flag,
                      pay_period_start_date,
                      pay_period_end_date)
            VALUES (p_process_id,
                    l_person_id,
                    (l_pay_rate / 12),
                    160,
                    r_pay_period.end_day,
                    'Estimate',
                    p_ppt,
                    'NONTAX TEMP',
                    l_pay_frequency,
                    'T',
                    r_pay_period.start_day,
                    r_pay_period.end_day);

            /*
            insert into XXPEN_HR_SERVICE_TIME_LINES_PROJECTED_TBL (process_id,person_id,projected,adjusted_hours,adjusted_earnings,actual_hours,actual_earnings,service_credit,pay_period_start_date,pay_period_end_Date)
            values(p_process_id,l_person_id,'Y',160,(l_pay_rate/12),160,(l_pay_rate/12),'1',r_pay_period.start_day,r_pay_period.end_day); */
            l_previous_year := TO_CHAR (r_pay_period.end_day, 'YYYY');
            l_previous_year := TO_CHAR (r_pay_period.end_day, 'YYYY');
         END LOOP;
      ELSIF     l_pay_frequency = 'B'
            AND nvl(l_estimate_overrides_rec.termination_date,l_estimate_overrides_rec.EVENT_DATE) >= trunc(SYSDATE) AND l_estimate_overrides_rec.assume_earnings_per_year is not null
      THEN
         FOR r_pay_period
            IN c_semi_monthly_pay (
                  nvl(l_estimate_overrides_rec.termination_date,l_estimate_overrides_rec.EVENT_DATE))
         LOOP
            IF l_previous_year <> TO_CHAR (r_pay_period.end_day, 'YYYY')
            THEN
               l_pay_rate :=
                  ROUND (
                       l_pay_rate
                     * (  1
                        + (  NVL (
                                l_estimate_overrides_rec.assume_salary_increase_per_year,
                                0)
                           / 100)),
                     2);
            END IF;

            INSERT
              INTO xxpen_hr_projected_earn_hours_estimate_tbl (
                      process_id,
                      person_id,
                      earnings,
                      hours,
                      check_date,
                      data_source,
                      pension_plan_type,
                      non_taxable_earnings,
                      pay_frequency,
                      processed_flag,
                      pay_period_start_date,
                      pay_period_end_date)
            VALUES (p_process_id,
                    l_person_id,
                    (l_pay_rate / 26),
                    80,
                    r_pay_period.end_day,
                    'Estimate',
                    p_ppt,
                    'NONTAX TEMP',
                    l_pay_frequency,
                    'T',
                    r_pay_period.start_day,
                    r_pay_period.end_day);

            /*insert into XXPEN_HR_SERVICE_TIME_LINES_PROJECTED_TBL (process_id,person_id,projected,adjusted_hours,adjusted_earnings,actual_hours,actual_earnings,service_credit,pay_period_start_date,pay_period_end_Date)
            values(p_process_id,l_person_id,'Y',80,(l_pay_rate/26),80,(l_pay_rate/26),'1',r_pay_period.start_day,r_pay_period.end_day);  */
            l_previous_year := TO_CHAR (r_pay_period.end_day, 'YYYY');
         END LOOP;

         xxafw_process_monitor_pkg.log_info (
            '***ENDING PROJECTED_EARN_HOURS_ESTIMATE');
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         xxafw_process_monitor_pkg.log_info (
               '***ENDING PROJECTED_EARN_HOURS_ESTIMATE WITH ERRORS '
            || SUBSTR (SQLERRM, 1, 64));
   END projected_earn_hours_estimate;

   PROCEDURE get_optional_form_pagination (p_pagination IN VARCHAR2)
   IS
   BEGIN
      apex_collection.create_or_truncate_collection (
         p_collection_name => 'ESTIMATES_PAGINATION_OPTIONS');

      IF p_pagination = 'Summary – No DROP' OR p_pagination IS NULL
      THEN
         apex_collection.add_member (
            p_collection_name   => 'ESTIMATES_PAGINATION_OPTIONS',
            p_c001              => 'Normal Form');
         apex_collection.add_member (
            p_collection_name   => 'ESTIMATES_PAGINATION_OPTIONS',
            p_c001              => 'Option A');
         apex_collection.add_member (
            p_collection_name   => 'ESTIMATES_PAGINATION_OPTIONS',
            p_c001              => 'Option B');
         apex_collection.add_member (
            p_collection_name   => 'ESTIMATES_PAGINATION_OPTIONS',
            p_c001              => 'Option D');
         apex_collection.add_member (
            p_collection_name   => 'ESTIMATES_PAGINATION_OPTIONS',
            p_c001              => 'Option E');
         apex_collection.add_member (
            p_collection_name   => 'ESTIMATES_PAGINATION_OPTIONS',
            p_c001              => 'Option F');
      ELSIF p_pagination = 'Summary – Option C No DROP'
      THEN
         apex_collection.add_member (
            p_collection_name   => 'ESTIMATES_PAGINATION_OPTIONS',
            p_c001              => 'Option C');
         apex_collection.add_member (
            p_collection_name   => 'ESTIMATES_PAGINATION_OPTIONS',
            p_c001              => 'Normal Form');
      ELSIF p_pagination = 'Summary – 1x Year DROP'
      THEN
         apex_collection.add_member (
            p_collection_name   => 'ESTIMATES_PAGINATION_OPTIONS',
            p_c001              => 'Normal Form DROP (1x)');
         apex_collection.add_member (
            p_collection_name   => 'ESTIMATES_PAGINATION_OPTIONS',
            p_c001              => 'Option A DROP (1x)');
         apex_collection.add_member (
            p_collection_name   => 'ESTIMATES_PAGINATION_OPTIONS',
            p_c001              => 'Option B DROP (1x)');
         apex_collection.add_member (
            p_collection_name   => 'ESTIMATES_PAGINATION_OPTIONS',
            p_c001              => 'Option D DROP (1x)');
         apex_collection.add_member (
            p_collection_name   => 'ESTIMATES_PAGINATION_OPTIONS',
            p_c001              => 'Option E DROP (1x)');
         apex_collection.add_member (
            p_collection_name   => 'ESTIMATES_PAGINATION_OPTIONS',
            p_c001              => 'Option F DROP (1x)');
      ELSIF p_pagination = 'Summary – 2x Year DROP'
      THEN
         apex_collection.add_member (
            p_collection_name   => 'ESTIMATES_PAGINATION_OPTIONS',
            p_c001              => 'Normal Form DROP (2x)');
         apex_collection.add_member (
            p_collection_name   => 'ESTIMATES_PAGINATION_OPTIONS',
            p_c001              => 'Option A DROP (2x)');
         apex_collection.add_member (
            p_collection_name   => 'ESTIMATES_PAGINATION_OPTIONS',
            p_c001              => 'Option B DROP (2x)');
         apex_collection.add_member (
            p_collection_name   => 'ESTIMATES_PAGINATION_OPTIONS',
            p_c001              => 'Option D DROP (2x)');
         apex_collection.add_member (
            p_collection_name   => 'ESTIMATES_PAGINATION_OPTIONS',
            p_c001              => 'Option E DROP (2x)');
         apex_collection.add_member (
            p_collection_name   => 'ESTIMATES_PAGINATION_OPTIONS',
            p_c001              => 'Option F DROP (2x)');
      ELSIF p_pagination = 'Summary – 3x Year DROP'
      THEN
         apex_collection.add_member (
            p_collection_name   => 'ESTIMATES_PAGINATION_OPTIONS',
            p_c001              => 'Normal Form DROP (3x)');
         apex_collection.add_member (
            p_collection_name   => 'ESTIMATES_PAGINATION_OPTIONS',
            p_c001              => 'Option A DROP (3x)');
         apex_collection.add_member (
            p_collection_name   => 'ESTIMATES_PAGINATION_OPTIONS',
            p_c001              => 'Option B DROP (3x)');
         apex_collection.add_member (
            p_collection_name   => 'ESTIMATES_PAGINATION_OPTIONS',
            p_c001              => 'Option D DROP (3x)');
         apex_collection.add_member (
            p_collection_name   => 'ESTIMATES_PAGINATION_OPTIONS',
            p_c001              => 'Option E DROP (3x)');
         apex_collection.add_member (
            p_collection_name   => 'ESTIMATES_PAGINATION_OPTIONS',
            p_c001              => 'Option F DROP (3x)');
      ELSIF p_pagination = 'Summary – Option C 1x Year DROP'
      THEN
         apex_collection.add_member (
            p_collection_name   => 'ESTIMATES_PAGINATION_OPTIONS',
            p_c001              => 'Option C DROP (1x)');
         apex_collection.add_member (
            p_collection_name   => 'ESTIMATES_PAGINATION_OPTIONS',
            p_c001              => 'Normal Form DROP (1x)');
      ELSIF p_pagination = 'Summary – Option C 2x Year DROP'
      THEN
         apex_collection.add_member (
            p_collection_name   => 'ESTIMATES_PAGINATION_OPTIONS',
            p_c001              => 'Option C DROP (2x)');
         apex_collection.add_member (
            p_collection_name   => 'ESTIMATES_PAGINATION_OPTIONS',
            p_c001              => 'Normal Form DROP (2x)');
      ELSIF p_pagination = 'Summary – Option C 3x Year DROP'
      THEN
         apex_collection.add_member (
            p_collection_name   => 'ESTIMATES_PAGINATION_OPTIONS',
            p_c001              => 'Option C DROP (3x)');
         apex_collection.add_member (
            p_collection_name   => 'ESTIMATES_PAGINATION_OPTIONS',
            p_c001              => 'Normal Form DROP (3x)');
      END IF;
   END get_optional_form_pagination;

   PROCEDURE create_payment (
      p_payment_info_rec     IN xxpen_hr_payment_info_tbl%ROWTYPE,
      p_created_by           IN VARCHAR2,
      p_pension_start_date   IN DATE,
      p_payment_frequency    IN VARCHAR2)
   IS
      l_estimate_record           xxpen_hr_estimate_overrides_tbl%ROWTYPE;
      l_payment_info_rec          xxpen_hr_payment_info_tbl%ROWTYPE;
      l_pension_calc_attbr_rec    xxpen_hr_plan_calc_attr_tbl%ROWTYPE;
      l_pension_calculation_rec   xxpen_hr_pension_cal_tbl%ROWTYPE;
      l_payment_type              xxpen_hr_calculation_types_tbl.payment_type%TYPE;
      l_payment_id                xxpen_hr_payment_info_tbl.payment_id%TYPE;
   BEGIN
      --end set values
      l_payment_info_rec := p_payment_info_rec;

      DELETE FROM xxpen_hr_payment_status_tbl
            WHERE payment_id IN
                     (SELECT payment_id
                        FROM xxpen_hr_payment_info_tbl
                       WHERE     person_id = l_payment_info_rec.person_id
                             AND payment_end_date IS NULL
                             AND payment_type =
                                    l_payment_info_rec.payment_type);

      DELETE FROM xxpen_hr_payment_info_tbl
            WHERE     person_id = l_payment_info_rec.person_id
                  AND payment_end_date IS NULL
                  AND payment_type = l_payment_info_rec.payment_type;
      --write to tables create parent record in XXPEN_HR_PAYMENT_STATUS_TBL
      INSERT INTO xxpen_hr_payment_info_tbl
           VALUES l_payment_info_rec
           returning payment_id into l_payment_id;

      /*
      insert into XXPEN_HR_PAYMENT_STATUS_TBL(payment_id,effective_date,PAYMENT_SOURCE,created_by,payment_start_date,payment_frequency,creation_date,payment_status) values
      ("WKSP_PENSIONCALC"."ISEQ$$_149138".currval,sysdate,'System Calculated',p_created_by,p_pension_start_date,p_payment_frequency,sysdate,0);
  */
      INSERT INTO xxpen_hr_payment_status_tbl (payment_status,
                                               payment_source,
                                               payment_frequency,
                                               payment_start_date,
                                               payment_end_date,
                                               payment_id,
                                               payment_comment,
                                               annualized_amount,
                                               non_taxable_amount,
                                               payee_person_number,
                                               payment_total,
                                               payment_number,
                                               effective_date)
           VALUES ('0 System Generated',
                   l_payment_info_rec.payment_source,
                   l_payment_info_rec.pay_frequency,
                   l_payment_info_rec.payment_start_date,
                   l_payment_info_rec.payment_end_date,
                   l_payment_id,
                   l_payment_info_rec.payment_comment,
                   l_payment_info_rec.annualized_amount,
                   l_payment_info_rec.non_taxable_monthly_amt,
                   l_payment_info_rec.payee_person_number,
                   l_payment_info_rec.payment_total,
                   l_payment_info_rec.payment_number,
                   SYSDATE);
   END create_payment;

   FUNCTION get_non_taxable_amt (p_process_id          IN NUMBER,
                                 p_drop_monthly_flag   IN VARCHAR2,
                                 p_drop_selected       IN VARCHAR2,
                                 p_optional_form       IN VARCHAR2)
      RETURN VARCHAR2
   IS
      l_pension_calculation_rec   xxpen_hr_pension_cal_tbl%ROWTYPE;
      l_function_name             xxpen_hr_pension_cal_tbl.function_name%TYPE;
   BEGIN
      IF    p_optional_form LIKE '%Option A%'
         OR p_optional_form LIKE '%Option B%'
         OR p_optional_form LIKE '%Option E%'
         OR p_optional_form LIKE '%Option F%'
      THEN
         IF UPPER (p_drop_selected) LIKE '%1%'
         THEN
            l_function_name := 'Non Taxable Survivor Options with DROP (1x)';
         ELSIF UPPER (p_drop_selected) LIKE '%2%'
         THEN
            l_function_name := 'Non Taxable Survivor Options with DROP (2x)';
         ELSIF UPPER (p_drop_selected) LIKE '%3%'
         THEN
            l_function_name := 'Non Taxable Survivor Options with DROP (3x)';
         ELSE
            l_function_name := 'Non Taxable Survivor Options No DROP';
         END IF;

         SELECT *
           INTO l_pension_calculation_rec
           FROM xxpen_hr_pension_cal_tbl
          WHERE process_id = p_process_id AND function_name = l_function_name;

         IF p_drop_monthly_flag = 'D'
         THEN
            RETURN NVL (
                      l_pension_calculation_rec.surv_non_taxable_drop_lumpsum_amt,
                      '0');
         ELSIF p_drop_monthly_flag = 'M'
         THEN
            RETURN NVL (
                      l_pension_calculation_rec.surv_non_taxable_monthly_amount,
                      '0');
         END IF;
      ELSE
         IF UPPER (p_drop_selected) LIKE '%1%'
         THEN
            l_function_name := 'Non Taxable Retiree Options with DROP (1x)';
         ELSIF UPPER (p_drop_selected) LIKE '%2%'
         THEN
            l_function_name := 'Non Taxable Retiree Options with DROP (2x)';
         ELSIF UPPER (p_drop_selected) LIKE '%3%'
         THEN
            l_function_name := 'Non Taxable Retiree Options with DROP (3x)';
         ELSE
            l_function_name := 'Non Taxable Retiree Options No DROP';
         END IF;

         SELECT *
           INTO l_pension_calculation_rec
           FROM xxpen_hr_pension_cal_tbl
          WHERE process_id = p_process_id AND function_name = l_function_name;

         IF p_drop_monthly_flag = 'D'
         THEN
            RETURN NVL (
                      l_pension_calculation_rec.non_taxable_drop_lumpsum_amt,
                      '0');
         ELSIF p_drop_monthly_flag = 'M'
         THEN
            RETURN NVL (l_pension_calculation_rec.non_taxable_monthly_amount,
                        '0');
         END IF;
      END IF;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN '';
   END get_non_taxable_amt;

    PROCEDURE BULK_ESTIMATE (
        p_pension_start_date IN date,
        p_earnings_assumptions IN varchar2,
        p_assume_salary_increase_per_year IN NUMBER,
        p_person_id IN varchar2 default null,
        p_employment_status IN varchar2 default null,
        p_time_frame IN number default null
    )
    is
        l_assume_earnings_per_year number := 0;
        l_assume_hours_per_year number := 0;

        --process monitor variables
        l_pension_participant_number varchar2(255);
        l_pension_participant_plan varchar2(255);
        l_calculation_name varchar2(1000);
        l_calculation_source varchar2(255);
        l_calculation_description varchar2(255);

        l_estimate_record xxpen_hr_estimate_overrides_tbl%ROWTYPE;
        l_process_id number;

        --process variables
        l_estimate_participants_table  estimate_pension_participants_t;
        l_estimate_participants_record estimate_pension_participants_rt;
        c_limit PLS_INTEGER := 1000;

        CURSOR estimate_participants_cur is
            SELECT 
                pp.pension_participants_id,
                pp.person_id,
                pp.person_number,
                NVL(pp.annual_base_pay, 0) AS annual_base_pay,
                pp.employment_status,
                pp.effective_date
            FROM (
                SELECT 
                    pp.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY pp.person_id 
                        ORDER BY pp.effective_date DESC
                    ) AS rn
                FROM XXPEN_HR_PENSION_PARTICIPANTS_TBL pp
                WHERE NOT EXISTS (
                    SELECT 1 
                    FROM xxpen_hr_estimate_overrides_tbl eo
                    WHERE eo.person_id = pp.person_id 
                      AND eo.calculation_name LIKE 'FIN_%'
                )
            ) pp
            WHERE pp.rn = 1 --flag for using only the latest effective_date pp table record
              AND pp.person_type IN ('Employee','Charter School Worker')
              AND (
                p_person_id is NULL OR pp.person_id 
                in (
                    Select column_value AS val
                    FROM TABLE(apex_string.split(p_person_id, ':'))
                )
              )
              AND (
                p_employment_status is NULL OR pp.employment_status 
                in (
                    Select column_value AS val
                    FROM TABLE(apex_string.split(p_employment_status, ':'))
                )
              )
              AND (
                /* p_time_frame IS NULL OR person_id NOT IN (
                    select person_id from (
                        SELECT person_id,
                               (SYSDATE - CREATION_DATE) * 24 AS total_hours
                        FROM XXAFW_PROCESS_MONITOR_TBL
                        WHERE CALCULATION_SOURCE = 'Estimate Admin'
                    )
                    WHERE total_hours < p_time_frame
                ) */
                p_time_frame IS NULL OR NOT EXISTS (
                    SELECT 1
                    FROM XXAFW_PROCESS_MONITOR_TBL pm
                    WHERE 1=1 
                      AND pm.person_id = pp.person_id
                      AND pm.CALCULATION_SOURCE = 'Estimate Admin'
                      AND (SYSDATE - pm.CREATION_DATE) * 24 < p_time_frame
                )
              );
              --AND ROWNUM <= 500; --just for testing

    begin

        OPEN estimate_participants_cur;
        LOOP
            FETCH estimate_participants_cur
            BULK COLLECT INTO l_estimate_participants_table
            LIMIT c_limit;  

            EXIT WHEN l_estimate_participants_table.COUNT = 0;

            FOR indx IN 1 .. l_estimate_participants_table.COUNT
            LOOP
                l_estimate_participants_record := l_estimate_participants_table(indx);

                --default NO_PROJECTED       
                l_assume_earnings_per_year := 0;
                l_assume_hours_per_year := 0;

                --Assume Earnings Per Year should be that person's Compensation Rate Annual Amount
                if p_earnings_assumptions = 'USE_CURRENT_SALARY' then

                    l_assume_earnings_per_year := l_estimate_participants_record.ANNUAL_BASE_PAY;
                    l_assume_hours_per_year := 2080;

                end if;

                XXPEN_PENSION_CALCULATIONS_PKG.get_current_pension_participants_info(
                    p_pension_participants_id => l_estimate_participants_record.pension_participants_id,
                    o_pension_participants_number => l_pension_participant_number,
                    o_pension_participants_plan => l_pension_participant_plan
                );

                l_pension_participant_number := l_estimate_participants_record.person_number;
                l_calculation_name := 'EST_' || to_char(SYSDATE, 'YYYYMMDD') || '_EE' || l_pension_participant_number || '_Estimate_Admin_ID' || XXPEN_HR_CALCULATION_ID_SEQ.NEXTVAL;
                l_calculation_source := 'Estimate Admin';
                l_calculation_description := 'Admin Mass Estimate';

                XXAFW_PROCESS_MONITOR_PKG.g_header := true;
                XXAFW_PROCESS_MONITOR_PKG.init_new_pension_calculation_monitor(
                    p_calculation_source => l_calculation_source,
                    p_calculation_name => l_calculation_name,
                    p_pension_plan => l_pension_participant_plan,
                    p_person_id => l_estimate_participants_record.person_id,
                    p_person_number => l_pension_participant_number,
                    p_calculation_description => l_calculation_description
                );

                XXAFW_PROCESS_MONITOR_PKG.log_info ('Bulk Estimate Parameters');
                XXAFW_PROCESS_MONITOR_PKG.log_info ('Pension Start Date ' || p_pension_start_date);
                XXAFW_PROCESS_MONITOR_PKG.log_info ('Earnings Assumptions ' || p_earnings_assumptions);

                if p_time_frame IS NOT NULL then
                    XXAFW_PROCESS_MONITOR_PKG.log_info ('Time Frame is Last ' || p_time_frame || ' hours');
                end if;

                if p_assume_salary_increase_per_year IS NOT NULL then
                    XXAFW_PROCESS_MONITOR_PKG.log_info ('Assume Salary Increase Per Year ' || p_assume_salary_increase_per_year || '%');
                end if;
                if p_person_id IS NOT NULL then
                    XXAFW_PROCESS_MONITOR_PKG.log_info ('Person IDS ' || REPLACE(p_person_id,':',' - '));
                end if;
                if p_employment_status IS NOT NULL then
                    XXAFW_PROCESS_MONITOR_PKG.log_info ('Employment Status ' || REPLACE(p_employment_status,':',' - '));
                end if;
                XXAFW_PROCESS_MONITOR_PKG.log_info ('');

                l_estimate_record.person_id := l_estimate_participants_record.person_id;
                l_estimate_record.calculation_source := l_calculation_source;
                l_estimate_record.event_reason := 'Estimate Admin';
                l_estimate_record.event_date := p_pension_start_date - 1;
                l_estimate_record.pension_start_date := p_pension_start_date;
                l_estimate_record.calculation_options := 'ALL_ESTIMATE';
                l_estimate_record.calculation_description := l_calculation_description;

                l_estimate_record.CALCULATION_LOCKED := 'No';
                l_estimate_record.assume_earnings_per_year := Replace(l_assume_earnings_per_year,',','');
                l_estimate_record.assume_hours_per_year := l_assume_hours_per_year;
                l_estimate_record.process_id := XXAFW_PROCESS_MONITOR_PKG.g_monitor_process_id;
                l_estimate_record.calculation_name := l_calculation_name;
                l_estimate_record.person_number := l_estimate_participants_record.person_number;
                l_estimate_record.effective_date := l_estimate_participants_record.effective_date;
                l_estimate_record.assume_salary_increase_per_year := p_assume_salary_increase_per_year;
                BEGIN
                    select nvl(pension_number,1)
                    into l_estimate_record.pension_number
                    from  XXPEN_HR_COS_TBL 
                    where person_id = l_estimate_record.person_id and cos_id = (select max(cos_id) from XXPEN_HR_COS_TBL where person_id = l_estimate_record.person_id);
                EXCEPTION WHEN OTHERS THEN
                    l_estimate_record.pension_number := 1;
                END;
                --l_estimate_record.optional_form_selected := :P53_OPTIONAL_FORM_SELECTED;
                --l_estimate_record.compensation_rate_annual_amt := REPLACE(:P53_COMPENSATION_RATE_ANNUAL_AMT,',','');
                --l_estimate_record.override_erd_assum := :P53_OVERRIDE_ERD_ASSUM;
                --l_estimate_record.override_pension_eligible := :P53_OVERRIDE_PENSION_ELIGIBLE_ASSUM;
                --l_estimate_record.override_urd_assum := :P53_OVERRIDE_URD_ASSUM;
                --l_estimate_record.override_accrued_service_credits := :P53_OVERRIDE_ACCRUED_SERVICE_CREDITS;
                --l_estimate_record.override_employee_dob := :P53_OVERRIDE_EMPLOYEE_DOB;
                --l_estimate_record.override_beneficiary_dob := :P53_OVERRIDE_BENEFICIARY_DOB;                
                --l_estimate_record.option_c_start_age := :P53_SS_START_AGE;
                --l_estimate_record.option_c_amount := :P53_SS_AMOUNT;
                --l_estimate_record.early_retirement_date := :P53_EARLY_RETIREMENT_DATE;
                --l_estimate_record.normal_retirement_date := :P53_UNREDUCED_RETIREMENT_DATE;
                --l_estimate_record.option_c_start_date := :P53_SS_START_DATE;
                --l_estimate_record.drop_election := :P53_DROP_ELECTION;

                XXXPEN_ESTIMATE_PKG.submit_estimate(l_estimate_record,l_process_id);
                COMMIT;

            END LOOP;
        END LOOP;
    end;

    PROCEDURE SCHEDULE_BULK_ESTIMATE(
        p_pension_start_date IN date,
        p_earnings_assumptions IN varchar2,
        p_assume_salary_increase_per_year IN NUMBER,
        p_person_id IN varchar2 default null,
        p_employment_status IN varchar2 default null,
        p_time_frame IN number default null
    )
    is
        l_action CLOB default empty_clob;
        l_program_name varchar2(255);

        e_exists EXCEPTION;
        PRAGMA EXCEPTION_INIT(e_exists,-27477);

        l_job_exists VARCHAR2(1000);
        l_message VARCHAR2(1000);
        l_message_error VARCHAR2(1000);
    begin
        
        l_program_name := 'BULK_ESTIMATE';
        l_action := 'begin XXXPEN_ESTIMATE_PKG.BULK_ESTIMATE (';

        l_action := l_action || 'p_pension_start_date => ''' || p_pension_start_date || ''',';
        l_action := l_action || 'p_earnings_assumptions => ''' || p_earnings_assumptions || ''',';
        l_action := l_action || 'p_assume_salary_increase_per_year => ''' || p_assume_salary_increase_per_year || ''',';
        l_action := l_action || 'p_person_id => ''' || p_person_id || ''',';
        l_action := l_action || 'p_employment_status => ''' || p_employment_status || ''',';
        l_action := l_action || 'p_time_frame => ''' || p_time_frame || '''';

        l_action := l_action || '); end;';

        -- create program
        begin

            XXAFW_PROCESS_MONITOR_PKG.scheduler_program_create(
                p_program_name   => l_program_name,
                p_program_action => l_action,
                p_program_type   => 'PLSQL_BLOCK',
                p_arguments      => 0,
                p_comments       => ''
            );

        exception
            when e_exists then
                    --program already exists, update action
                    begin
                        DBMS_SCHEDULER.drop_program_argument( 
                            program_name => l_program_name, 
                            argument_position => 1
                        );
                    exception
                        when others then
                        null;
                    end;
                    
                    -- enable program
                    DBMS_SCHEDULER.enable (l_program_name);

                    DBMS_SCHEDULER.set_attribute (
                       name           =>   l_program_name,
                       attribute      =>   'PROGRAM_ACTION',
                       value          =>   l_action
                    );
                null;
            --when others then
                --null;
        end;

        --delete schedule job that runs once to create a new one with the same name
        begin
            SELECT JOB_NAME into l_job_exists
                FROM USER_SCHEDULER_JOBS
                WHERE 1=1
                AND SCHEDULE_TYPE = 'ONCE'
                AND PROGRAM_NAME = l_program_name
                AND JOB_NAME = l_program_name || '_JOB';
                
            if l_job_exists IS NOT NULL then
                XXAFW_PROCESS_MONITOR_PKG.scheduler_drop_stop_job(
                    p_job_name => l_job_exists,
                    p_message => l_message,
                    p_message_error => l_message_error
                );
            end if;

            exception
                when no_data_found then
                    null;
        end;

        --schedule job
        XXAFW_PROCESS_MONITOR_PKG.scheduler_program_schedule_job(
            p_job_name      => l_program_name || '_JOB',
            p_program_name  => l_program_name,
            p_start_date    => localtimestamp,
            p_end_date      => '',
            p_repeat        => '',
            p_comments      => 'Adhoc Run',
            p_autodrop      => false,
            p_enabled       => true
        );
    end;

PROCEDURE SAVE_SERVICE_ESTIMATE_DETAILS(p_person_id in number,p_process_id in number) is
l_vesting_status varchar2(240);
BEGIN 
insert into XXPEN_HR_SERVICE_TIME_HEADERS_ESTIMATE_TBL(process_id,person_id,service_header_id,pension_service_points,thru_date,accu_service_breaks,ser_amt_with_breaks,accrued_service_credits,total_credited_service_month,total_credited_service_years,non_connected_hours,ghost_years,created_by,creation_date,last_updated_by,last_update_date,split_service_flag,split_service_category,split_service_plan_type,points_of_age,points_years_of_service,pension_plan_type,service_credit_start_date,total_credited_Service_months_actual,years_of_service,evaluation_end_date,eligible_credit_count,vesting_credit_count,credit_evaluation,eligibility_date,split_pf_points,split_total_credit_months_actual,split_total_credit_years_actual,cos_credit_count,total_credited_service_years_actual,early_retirement_date,normal_retirement_date,pension_number,ERD_DATE_SPLIT_CAT3_PF,URD_DATE_SPLIT_CAT3_PF)
select p_process_id,person_id,service_header_id,pension_service_points,thru_date,accu_service_breaks,ser_amt_with_breaks,accrued_service_credits,total_credited_service_month,total_credited_service_years,non_connected_hours,ghost_years,created_by,creation_date,last_updated_by,last_update_date,split_service_flag,split_service_category,split_service_plan_type,points_of_age,points_years_of_service,pension_plan_type,service_credit_start_date,total_credited_Service_months_actual,years_of_service,evaluation_end_date,eligible_credit_count,vesting_credit_count,credit_evaluation,eligibility_date,split_pf_points,split_total_credit_months_actual,split_total_credit_years_actual,cos_credit_count,total_credited_service_years_actual,early_retirement_date,normal_retirement_date,pension_number,ERD_DATE_SPLIT_CAT3_PF,URD_DATE_SPLIT_CAT3_PF
from XXPEN_HR_SERVICE_TIME_HEADERS_TBL  where person_id = p_person_id;

insert into XXPEN_HR_SERVICE_TIME_LINES_ESTIMATE_TBL(service_line_id,service_header_id,person_id,pay_period_start_date,pay_period_end_date,actual_hours,adjusted_hours,actual_earnings,adjusted_earnings,limited_applied,projected,service_credit,service_break,accrued_service_credits,accumulated_service_breaks,exclude_from_fae,created_by,creation_date,last_updated_by,last_update_date,credit_evaluation,used_in_fae_last_year,skipped_in_fae,used_in_fae_five_years,process_id,emp_period_number,pension_number)
select service_line_id,service_header_id,person_id,pay_period_start_date,pay_period_end_date,actual_hours,adjusted_hours,actual_earnings,adjusted_earnings,limited_applied,projected,service_credit,service_break,accrued_service_credits,accumulated_service_breaks,exclude_from_fae,created_by,creation_date,last_updated_by,last_update_date,credit_evaluation,used_in_fae_last_year,skipped_in_fae,used_in_fae_five_years,p_process_id as process_id,emp_period_number,pension_number
from XXPEN_HR_SERVICE_TIME_LINES_VIEW where person_id = p_person_id and (process_id = p_process_id or process_id is null);

insert into XXPEN_HR_FINAL_AVG_EAR_ESTIMATE_TBL (process_id,fae_id,person_id,year,event_date,five_year_start_date,five_year_end_date,five_year_monthly_amount,five_year_annual_amount,five_year_total_amount,last_year_start_date,last_year_end_date,last_year_monthly_amount,last_year_total,last_year_25_percent,created_by,creation_date,last_updated_by,last_update_date,limit_applied,consecutive_count)
select p_process_id,fae_id,person_id,year,event_date,five_year_start_date,five_year_end_date,five_year_monthly_amount,five_year_annual_amount,five_year_total_amount,last_year_start_date,last_year_end_date,last_year_monthly_amount,last_year_total,last_year_25_percent,created_by,creation_date,last_updated_by,last_update_date,limit_applied,consecutive_count 
from XXPEN_HR_FINAL_AVG_EAR_TBL
where person_id = p_person_id;
    BEGIN
    select
        distinct
        REPLACE(INITCAP(ppt.VESTING_STATUS),'_',' ')
    into
    l_vesting_status
    from
        xxpen_hr_pension_participants_tbl ppt
    where
        ppt.person_id = p_person_id
        and ppt.effective_date = (select max(effective_date) from xxpen_hr_pension_participants_tbl where person_id = p_person_id) and rownum = 1;
    EXCEPTION WHEN NO_DATA_FOUND THEN
    l_vesting_status := 'Error Retrieving Vesting Status';
    END;
update xxpen_hr_estimate_overrides_tbl set vesting_status = l_vesting_status where process_id = p_process_id;
END SAVE_SERVICE_ESTIMATE_DETAILS;
PROCEDURE RERUN_PREVIOUS_ESTIMATE(p_process_id in number,p_new_process_id out number,p_calc_name out varchar2) AS
l_estimate_record xxpen_hr_estimate_overrides_tbl%ROWTYPE;
l_process_id number := p_process_id;
BEGIN  
SELECT * 
INTO l_estimate_record 
FROM xxpen_hr_estimate_overrides_tbl where process_id = l_process_id;
--get new optional form selected survivor and drop election if available start

        
        select distinct pbi.OPTIONAL_FORM_SELECTED

        into

            l_estimate_record.optional_form_selected
        from
            xxpen_hr_plan_benefit_info_tbl pbi,
            xxpen_hr_pension_participants_tbl ppt,
            XXPEN_HR_PLAN_CALC_ATTR_TBL pi,
            xxpen_hr_service_time_headers_tbl sth

        where
            ppt.person_id = pbi.person_id
        and ppt.effective_date = pbi.effective_date
        and ppt.pension_participants_id = pi.person_id(+)
        and ppt.effective_date = pi.effective_date(+)
        and ppt.person_id = sth.person_id(+)
        -- and ppt.pension_participants_id = :P0_XXPEN_HR_PENSION_PARTICIPANTS_TBL_ID;
        and ppt.person_id = l_estimate_record.person_id
        and sth.creation_date = (select max(creation_date) from xxpen_hr_service_time_headers_tbl where person_id = l_estimate_record.person_id)
        and ppt.effective_date = (select max(effective_date) from xxpen_hr_pension_participants_tbl where person_id = l_estimate_record.person_id) and rownum = 1;

       declare
        l_effective_date date;
        begin
        select max(effective_date) into l_effective_date from XXPEN_HR_PLAN_CALC_ATTR_TBL where person_id = l_estimate_record.person_id;
        select 

               REPLACE(REPLACE(INITCAP(DROP_ELECTION),'_',' '),'Drop','DROP')       
          into 
               l_estimate_record.drop_election

        from XXPEN_HR_PLAN_CALC_ATTR_TBL
         where person_id = l_estimate_record.person_id
         and effective_date = l_effective_date;
  exception
        when others
        then
                              l_estimate_record.drop_election := NULL;
    end;
--get new optional form selected survivor and drop election if available end
select SUBSTR(calculation_name, 1, INSTR(calculation_name, 'ID') - 1)||'ID'||XXPEN_HR_ESTIMATE_OVERRIDES_SEQ.NEXTVAL 
into l_estimate_record.calculation_name
from xxpen_hr_estimate_overrides_tbl where process_id = l_process_id;
p_calc_name := l_estimate_record.calculation_name;
XXXPEN_ESTIMATE_PKG.submit_estimate(l_estimate_record,p_new_process_id);
END;   
FUNCTION GET_OPTIONAL_FORM_SELECTED(p_optional_form in varchar2 ,p_drop_election in varchar2) RETURN VARCHAR2 is
l_optional_form varchar2(240);
BEGIN
IF UPPER(p_optional_form) like '%ALL%' THEN
            l_optional_form := 'Normal Form';
        ELSIF UPPER(p_optional_form) like '%NORMAL%' THEN
            l_optional_form := 'Normal Form';
        ELSIF UPPER(p_optional_form) like '%OPTION_A_100%' THEN
            if p_drop_election like '%1 Year%' then
                l_optional_form := 'Option A DROP (1x)';
            elsif p_drop_election like '%2 Year%' then
                l_optional_form := 'Option A DROP (2x)';
            elsif p_drop_election like '%3 Year%' then
                l_optional_form := 'Option A DROP (3x)';
            else l_optional_form := 'Option A';
            end if;
        ELSIF UPPER(p_optional_form) like '%OPTION_B_50%' THEN
            if p_drop_election like '%1 Year%' then
                l_optional_form := 'Option B DROP (1x)';
            elsif p_drop_election like '%2 Year%' then
                l_optional_form := 'Option B DROP (2x)';
            elsif p_drop_election like '%3 Year%' then
                l_optional_form := 'Option B DROP (3x)';
            else l_optional_form := 'Option B';
            end if;
        ELSIF UPPER(p_optional_form) like '%OPTION_C_SSLIO%' THEN
            if p_drop_election like '%1 Year%' then
                l_optional_form := 'Option C DROP (1x)';
            elsif p_drop_election like '%2 Year%' then
                l_optional_form := 'Option C DROP (2x)';
            elsif p_drop_election like '%3 Year%' then
                l_optional_form := 'Option C DROP (3x)';
            else l_optional_form := 'Option C';
            end if;
        ELSIF UPPER(p_optional_form) like '%OPTION_D_120_GUAR%' THEN
            if p_drop_election like '%1 Year%' then
                l_optional_form := 'Option D DROP (1x)';
            elsif p_drop_election like '%2 Year%' then
                l_optional_form := 'Option D DROP (2x)';
            elsif p_drop_election like '%3 Year%' then
                l_optional_form := 'Option D DROP (3x)';
            else l_optional_form := 'Option D';
            end if;
        ELSIF UPPER(p_optional_form) like '%OPTION_E_100_POP%' THEN
            if p_drop_election like '%1 Year%' then
                l_optional_form := 'Option E DROP (1x)';
            elsif p_drop_election like '%2 Year%' then
                l_optional_form := 'Option E DROP (2x)';
            elsif p_drop_election like '%3 Year%' then
                l_optional_form := 'Option E DROP (3x)';
            else l_optional_form := 'Option E';
            end if;
        ELSIF UPPER(p_optional_form) like '%OPTION_F_50_POP%' THEN         
            if p_drop_election like '%1 Year%' then
                l_optional_form := 'Option F DROP (1x)';
            elsif p_drop_election like '%2 Year%' then
                l_optional_form := 'Option F DROP (2x)';
            elsif p_drop_election like '%3 Year%' then
                l_optional_form := 'Option F DROP (3x)';
            else l_optional_form := 'Option F';
            end if;
        ELSE
            if p_drop_election like '%1 Year%' then
                l_optional_form := 'DROP New Normal (1x)';
            elsif p_drop_election like '%2 Year%' then
                l_optional_form := 'DROP New Normal (2x)';
            elsif p_drop_election like '%3 Year%' then
                l_optional_form := 'DROP New Normal (3x)';
            else l_optional_form := 'Normal Form';
            end if;
END IF;
RETURN l_optional_form;
END;
END "XXXPEN_ESTIMATE_PKG";
/