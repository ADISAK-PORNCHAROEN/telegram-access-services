(() => {
  const token = window.location.hash.slice(1);
  if (token) history.replaceState(null, "", window.location.pathname);

  const panels = {
    phone: document.querySelector("#phone-form"),
    code: document.querySelector("#code-form"),
    password: document.querySelector("#password-form"),
    complete: document.querySelector("#complete-panel"),
  };
  const message = document.querySelector("#message");
  const missingToken = document.querySelector("#missing-token");
  const counter = document.querySelector("#step-counter");
  const messages = {
    invalid_phone: "หมายเลขโทรศัพท์ไม่ถูกต้อง ลองตรวจรหัสประเทศแล้วส่งอีกครั้ง",
    invalid_code: "รหัสไม่ถูกต้อง ตรวจสอบข้อความจาก Telegram แล้วลองอีกครั้ง",
    expired_code: "รหัสหมดอายุแล้ว ขอรหัสใหม่ก่อนกรอกอีกครั้ง",
    invalid_password: "รหัสผ่านสองชั้นไม่ถูกต้อง ลองใหม่อีกครั้ง",
    rate_limited: "Telegram จำกัดความถี่ กรุณารอสักครู่แล้วลองใหม่",
    unavailable: "เชื่อมต่อ Telegram ไม่สำเร็จ ตรวจการเชื่อมต่อแล้วลองใหม่",
    invalid_request: "ข้อมูลที่กรอกไม่ครบหรือรูปแบบไม่ถูกต้อง",
  };

  if (!token) {
    missingToken.hidden = false;
    Object.values(panels).forEach((panel) => { panel.hidden = true; });
    return;
  }

  const showMessage = (text, error = false) => {
    message.textContent = text;
    message.classList.toggle("message-error", error);
    message.hidden = !text;
  };

  const render = (state) => {
    Object.values(panels).forEach((panel) => { panel.hidden = true; });
    if (state.phase === "complete") {
      panels.complete.hidden = false;
      counter.innerHTML = "03 <i>/</i> 03";
      return;
    }
    panels[state.phase]?.removeAttribute("hidden");
    const step = state.phase === "phone" ? "01" : state.phase === "code" ? "02" : "03";
    counter.innerHTML = `${step} <i>/</i> 03`;
    if (state.issue) {
      const suffix = state.retryAfterSeconds ? ` (ลองอีกครั้งใน ${state.retryAfterSeconds} วินาที)` : "";
      showMessage((messages[state.issue] || messages.unavailable) + suffix, true);
    } else {
      showMessage("");
    }
  };

  const send = async (path, body = {}) => {
    const response = await fetch(path, {
      method: "POST",
      headers: { "Authorization": `Bearer ${token}`, "Content-Type": "application/json" },
      body: JSON.stringify(body),
      cache: "no-store",
      credentials: "same-origin",
      referrerPolicy: "no-referrer",
    });
    if (response.status === 401) throw new Error("ลิงก์ตั้งค่านี้ใช้แล้วหรือหมดอายุ กรุณาเปิดลิงก์ล่าสุดจาก log");
    const result = await response.json();
    if (!response.ok) throw new Error(messages[result.issue] || messages.unavailable);
    render(result);
  };

  const bind = (form, path, field) => {
    form.addEventListener("submit", async (event) => {
      event.preventDefault();
      const input = form.elements[field];
      const button = form.querySelector("button[type='submit']");
      button.disabled = true;
      try {
        await send(path, { [field]: input.value });
      } catch (error) {
        showMessage(error.message, true);
      } finally {
        if (field === "code" || field === "password") input.value = "";
        button.disabled = false;
      }
    });
  };

  bind(panels.phone, "/api/phone", "phone");
  bind(panels.code, "/api/code", "code");
  bind(panels.password, "/api/password", "password");
  document.querySelector("#resend-button").addEventListener("click", async () => {
    try {
      await send("/api/resend");
    } catch (error) {
      showMessage(error.message, true);
    }
  });
})();
