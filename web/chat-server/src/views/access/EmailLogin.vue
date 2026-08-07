<template>
  <div class="auth-wrap">
    <div class="auth-card">
      <h2 class="auth-title">验证码登录</h2>
      <p class="auth-subtitle">使用邮箱验证码快速登录</p>
      <el-form ref="formRef" :model="loginData" class="auth-form" @keyup.enter="handleEmailLogin">
        <el-form-item
          prop="email"
          :rules="[
            {
              required: true,
              message: '此项为必填项',
              trigger: 'blur',
            },
          ]"
        >
          <el-input
            v-model="loginData.email"
            placeholder="邮箱"
            size="large"
            :prefix-icon="'Message'"
          />
        </el-form-item>
        <el-form-item
          prop="email_code"
          :rules="[
            {
              required: true,
              message: '此项为必填项',
              trigger: 'blur',
            },
          ]"
        >
          <el-input
            v-model="loginData.email_code"
            placeholder="验证码"
            size="large"
            :prefix-icon="'Key'"
          >
            <template #append>
              <el-button
                @click="sendEmailCode"
                style="background-color: var(--brand); color: #ffffff; border: none"
                >点击发送</el-button
              >
            </template>
          </el-input>
        </el-form-item>
        <el-button type="primary" class="auth-btn" @click="handleEmailLogin"
          >登 录</el-button
        >
      </el-form>
      <div class="auth-links">
        <button class="auth-link-btn" @click="handleRegister">注册账号</button>
        <button class="auth-link-btn" @click="handleLogin">密码登录</button>
      </div>
    </div>
  </div>
</template>

<script>
import { reactive, toRefs } from "vue";
import axios from "@/api";
import { useRouter } from "vue-router";
import { ElMessage } from "element-plus";
import { useStore } from "vuex";
import { checkEmailValid } from "@/assets/js/valid.js";
export default {
  name: "EmailLogin",
  setup() {
    const data = reactive({
      loginData: {
        email: "",
        email_code: "",
      },
    });
    const router = useRouter();
    const store = useStore();
    const handleEmailLogin = async () => {
      try {
        if (!data.loginData.email || !data.loginData.email_code) {
          ElMessage.error("请填写完整登录信息。");
          return;
        }
        if (!checkEmailValid(data.loginData.email)) {
          ElMessage.error("请输入有效的邮箱地址。");
          return;
        }
        const response = await axios.post(
          store.state.backendUrl + "/user/emailLogin",
          data.loginData
        );
        console.log(response);
        if (response.data.code == 200) {
          if (response.data.data.status == 1) {
            ElMessage.error("该账号已被封禁，请联系管理员。");
            return;
          }
          try {
            ElMessage.success(response.data.message);
            if (!response.data.data.avatar.startsWith("http")) {
              response.data.data.avatar =
                store.state.backendUrl + response.data.data.avatar;
            }
            store.commit("setUserInfo", response.data.data);
            const wsUrl =
              store.state.wsUrl + "/wss?token=" + response.data.data.token;
            console.log(wsUrl);
            store.state.socket = new WebSocket(wsUrl);
            store.state.socket.onopen = () => {
              console.log("WebSocket连接已打开");
            };
            store.state.socket.onmessage = (message) => {
              console.log("收到消息：", message.data);
            };
            store.state.socket.onclose = () => {
              console.log("WebSocket连接已关闭");
            };
            store.state.socket.onerror = () => {
              console.log("WebSocket连接发生错误");
            };
            router.push("/chat/sessionlist");
          } catch (error) {
            console.log(error);
          }
        } else {
          ElMessage.error(response.data.message);
        }
      } catch (error) {
        ElMessage.error(error);
      }
    };
    const handleRegister = () => {
      router.push("/register");
    };
    const handleLogin = () => {
      router.push("/login");
    };
    const sendEmailCode = async () => {
      if (!data.loginData.email) {
        ElMessage.error("请输入邮箱地址。");
        return;
      }
      if (!checkEmailValid(data.loginData.email)) {
        ElMessage.error("请输入有效的邮箱地址。");
        return;
      }
      try {
        const req = {
          email: data.loginData.email,
        };
        const rsp = await axios.post(
          store.state.backendUrl + "/user/sendEmailCode",
          req
        );
        console.log(rsp);
        if (rsp.data.code == 200) {
          ElMessage.success(rsp.data.message);
        } else if (rsp.data.code == 400) {
          ElMessage.warning(rsp.data.message);
        } else {
          ElMessage.error(rsp.data.message);
        }
      } catch (error) {
        console.error(error);
      }
    };

    return {
      ...toRefs(data),
      router,
      handleEmailLogin,
      handleLogin,
      handleRegister,
      sendEmailCode,
    };
  },
};
</script>

<style>
/* 认证页样式统一由全局 assets/css/chat.css 的 .auth-* 提供 */
</style>