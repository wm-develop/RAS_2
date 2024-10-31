from datetime import datetime, timedelta


class TimeFormatConverter:
    def convert(self, time: str, filetype: str):
        """
        p01文件要求的时间格式为：字符串类型，DDMMMYYYY,HHMM
        b01和p01.tmp.hdf文件要求的时间格式为：字符串类型，DDMmmYYYY HHMM
        :param time: 数据库需要的标准时间格式，字符串类型，按照yyyy-mm-dd hh:00格式，为整点时刻数据
        :param filetype: 要转换成哪个文件的时间格式？只能是p01、b01或hdf
        :return: 返回p01文件要求的时间格式
        """
        time_format_list = time.split('-')
        year = time_format_list[0]
        month = time_format_list[1]
        day = time_format_list[2].split(' ')[0]
        hour = time_format_list[2].split(' ')[1].split(':')[0]
        minute = time_format_list[2].split(' ')[1].split(':')[1]

        if filetype == 'p01':
            month = self.__month_to_en(month).upper()
            p01_time_format = f"{day}{month}{year},{hour}{minute}"
            return p01_time_format
        elif filetype == 'b01' or filetype == 'hdf':
            if hour == '00':
                # 将时间调整为前一天的24:00
                date_time = datetime.strptime(time, "%Y-%m-%d %H:%M")
                date_time -= timedelta(days=1)
                day = date_time.strftime("%d")
                month = self.__month_to_en(date_time.strftime("%m"))
                year = date_time.strftime("%Y")  # 更新 year 为前一天的年份
                p01_time_format = f"{day}{month}{year} 2400"
            else:
                month = self.__month_to_en(month)
                p01_time_format = f"{day}{month}{year} {hour}{minute}"
            return p01_time_format
        elif filetype == 'simulation':
            # 输出格式为 DDMmmYYYY HH:MM:SS，不调整 00:00
            month = self.__month_to_en(month)
            simulation_time_format = f"{day}{month}{year} {hour}:{minute}:00"
            return simulation_time_format

    def __month_to_en(self, month: str):
        """
        将数字形式的月份转换为英文Mmm的形式
        :return: 返回月份对应的Mmm英文名
        """
        if month == '01':
            return 'Jan'
        elif month == '02':
            return 'Feb'
        elif month == '03':
            return 'Mar'
        elif month == '04':
            return 'Apr'
        elif month == '05':
            return 'May'
        elif month == '06':
            return 'Jun'
        elif month == '07':
            return 'Jul'
        elif month == '08':
            return 'Aug'
        elif month == '09':
            return 'Sep'
        elif month == '10':
            return 'Oct'
        elif month == '11':
            return 'Nov'
        elif month == '12':
            return 'Dec'

    def generate_result_timestep(self, start_time, end_time, interval=10):
        """
        生成从start_time到end_time的时间序列，间隔为interval分钟
        :param start_time: 起始时间，格式为YYYY-MM-DD HH:MM
        :param end_time: 结束时间，格式为YYYY-MM-DD HH:MM
        :param interval: 时间间隔（分钟）
        :return: list：包含时间步的列表
        """
        # 将字符串格式的时间转换为datetime对象
        start_dt = datetime.strptime(start_time, '%Y-%m-%d %H:%M')
        end_dt = datetime.strptime(end_time, '%Y-%m-%d %H:%M')

        # 初始化成果列表
        time_steps = []

        # 生成时间序列
        current_time = start_dt
        while current_time <= end_dt:
            time_steps.append(current_time.strftime('%Y-%m-%d %H:%M'))
            current_time += timedelta(minutes=interval)

        return time_steps

    def calculate_intervals(self, start_time, end_time, interval=10):
        # 将字符串格式的时间转换为 datetime 对象
        start = datetime.strptime(start_time, '%Y-%m-%d %H:%M')
        end = datetime.strptime(end_time, '%Y-%m-%d %H:%M')

        # 计算总时间差
        total_duration = end - start

        # 计算间隔数
        num_intervals = total_duration.total_seconds() // (interval * 60)

        return num_intervals


# 以下是用于测试的代码
if __name__ == '__main__':
    time_format_converter = TimeFormatConverter()
    p01_time = time_format_converter.convert('2023-03-26 09:00', 'hdf')
    print(p01_time)
