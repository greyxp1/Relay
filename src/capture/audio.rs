use std::{
    ffi::{c_char, CString},
    os::raw::c_void,
};

unsafe extern "C" {
    fn relay_share_audio_open(name: *const c_char, result: *mut *mut c_void) -> i32;
    fn relay_share_audio_error(router: *mut c_void) -> i32;
    fn relay_share_audio_free(router: *mut c_void);
}

pub(super) struct ShareAudio(pub(super) *mut c_void);

// All graph access is protected by PipeWire's thread loop lock, including destruction.
unsafe impl Send for ShareAudio {}

impl ShareAudio {
    pub(super) fn open(name: &str) -> Result<Self, String> {
        let name = CString::new(name).map_err(|e| e.to_string())?;
        let mut router = std::ptr::null_mut();
        let result = unsafe { relay_share_audio_open(name.as_ptr(), &mut router) };
        if result < 0 {
            return Err(format!(
                "Could not isolate share audio: {}",
                std::io::Error::from_raw_os_error(-result)
            ));
        }
        Ok(Self(router))
    }

    pub(super) fn error(&self) -> Option<String> {
        let error = unsafe { relay_share_audio_error(self.0) };
        (error != 0).then(|| std::io::Error::from_raw_os_error(error).to_string())
    }
}

impl Drop for ShareAudio {
    fn drop(&mut self) {
        unsafe { relay_share_audio_free(self.0) }
    }
}
