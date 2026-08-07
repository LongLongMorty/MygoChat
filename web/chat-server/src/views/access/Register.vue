<template>
  <div class="auth-wrap">
    <div class="auth-card">
      <h2 class="auth-title">注册</h2>
      <p class="auth-subtitle">创建你的 KamaChat 账号</p>
      <el-form ref="formRef" :model="registerData" class="auth-form" @keyup.enter="handleRegister">
        <el-form-item
          prop="nickname"
          :rules="[
            {
              required: true,
              message: '此项为必填项',
              trigger: 'blur',
            },
            {
              min: 3,
              max: 10,
              message: '昵称长度在 3 到 10 个字符',
              trigger: 'blur',
            },
          ]"
        >
          <el-input
            v-model="registerData.nickname"
            placeholder="昵称"
            size="large"
            :prefix-icon="'User'"
          />
        </el-form-item>
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
            v-model="registerData.email"
            placeholder="邮箱"
            size="large"
            :prefix-icon="'Message'"
          />
        </el-form-item>
        <el-form-item
          prop="password"
          :rules="[
            {
              required: true,
              message: '此项为必填项',
              trigger: 'blur',
            },
          ]"
        >
          <el-input
            type="password"
            v-model="registerData.password"
            placeholder="密码"
            size="large"
            show-password
            :prefix-icon="'Lock'"
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
            v-model="registerData.email_code"
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
        <el-button type="primary" class="auth-btn" @click="handleRegister"
          >注 册</el-button
        >
      </el-form>
      <div class="auth-links">
        <button class="auth-link-btn" @click="handleEmailLogin">
          验证码登录
        </button>
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
  name: "Register",
  setup() {
    const data = reactive({
      registerData: {
        email: "",
        password: "",
        nickname: "",
        email_code: "",
      },
    });
    const router = useRouter();
    const store = useStore();
    const handleRegister = async () => {
      try {
        if (
          !data.registerData.nickname ||
          !data.registerData.email ||
          !data.registerData.password ||
          !data.registerData.email_code
        ) {
          ElMessage.error("请填写完整注册信息。");
          return;
        }
        if (
          data.registerData.nickname.length < 3 ||
          data.registerData.nickname.length > 10
        ) {
          ElMessage.error("昵称长度在 3 到 10 个字符。");
          return;
        }
        if (!checkEmailValid(data.registerData.email)) {
          ElMessage.error("请输入有效的邮箱地址。");
          return;
        }
        const response = await axios.post(
          store.state.backendUrl + "/register",
          data.registerData
        );
        if (response.data.code == 200) {
          ElMessage.success(response.data.message);
          console.log(response.data.message);
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
        } else {
          ElMessage.error(response.data.message);
          console.log(response.data.message);
        }
      } catch (error) {
        ElMessage.error(error);
        console.log(error);
      }
    };

    const handleLogin = () => {
      router.push("/login");
    };

    const handleEmailLogin = () => {
      router.push("/emailLogin");
    };

    const sendEmailCode = async () => {
      if (
        !data.registerData.email ||
        !data.registerData.nickname ||
        !data.registerData.password
      ) {
        ElMessage.error("请填写完整注册信息。");
        return;
      }
      if (!checkEmailValid(data.registerData.email)) {
        ElMessage.error("请输入有效的邮箱地址。");
        return;
      }
      const req = {
        email: data.registerData.email,
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
    };

    return {
      ...toRefs(data),
      router,
      handleRegister,
      handleLogin,
      handleEmailLogin,
      sendEmailCode,
    };
  },
};
</script>

<style>
/* 认证页样式统一由全局 assets/css/chat.css 的 .auth-* 提供 */
</style>