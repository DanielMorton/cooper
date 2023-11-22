use std::path::PathBuf;

pub(super) struct FileMeta {
    year: u16,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    pub(super) channel: u8,
}

impl FileMeta {
    pub(super) fn new(pb: &PathBuf) -> Self {
        let file_name = match pb.file_name() {
            Some(f) => match f.to_str() {
                Some(fs) => fs,
                None => panic!("Unable to convert {:?} to string", f),
            },
            None => panic!("Missing file name for {:?}", pb),
        };
        let mut file_split = file_name.split('.').next().unwrap().split('_');
        let date = match file_split.next() {
            Some(d) => match d.parse::<u32>() {
                Ok(_) => d,
                Err(_) => match file_split.next() {
                    Some(d) => d,
                    None => panic!("No date in file name {}.", &file_name),
                },
            },
            None => panic!("No date in file name {}.", &file_name),
        };
        let year = match date[..4].parse::<u16>() {
            Ok(y) => y,
            Err(e) => panic!("{}", e),
        };
        let month = match date[4..6].parse::<u8>() {
            Ok(m) => m,
            Err(e) => panic!("{}", e),
        };
        let day = match date[6..8].parse::<u8>() {
            Ok(d) => d,
            Err(e) => panic!("{}", e),
        };

        let time = match file_split.next() {
            Some(t) => t,
            None => panic!("No time in file name {}.", &file_name),
        };
        let hour = match time[..2].parse::<u8>() {
            Ok(h) => h,
            Err(e) => panic!("{}", e),
        };
        let minute = match time[2..4].parse::<u8>() {
            Ok(m) => m,
            Err(e) => panic!("{}", e),
        };
        let second = match time[4..6].parse::<u8>() {
            Ok(s) => s,
            Err(e) => panic!("{}", e),
        };

        let channel = match file_split.next_back() {
            Some(c) => match c.parse::<u8>() {
                Ok(cint) => cint,
                Err(e) => panic!("{}", e),
            },
            None => 1,
        };
        FileMeta {
            year,
            month,
            day,
            hour,
            minute,
            second,
            channel,
        }
    }

    pub(super) fn get_date(&self) -> String {
        format!("{:04}-{:02}-{:02}", &self.year, &self.month, &self.day)
    }

    pub(super) fn get_time(&self) -> String {
        format!("{:02}:{:02}:{:02}", &self.hour, &self.minute, &self.second)
    }

    pub(super) fn get_time_of_day(&self) -> &str {
        if self.hour < 12 {
            "AM"
        } else {
            "PM"
        }
    }
}
