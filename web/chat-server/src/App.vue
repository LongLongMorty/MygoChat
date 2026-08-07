<template>
  <router-view />
</template>

<script>
import { onMounted } from "vue";
import { useRouter } from "vue-router";
import { useStore } from "vuex";
import { ElMessage } from "element-plus";
import axios from "@/api";
export default {
  name: "App",
  setup() {
    const router = useRouter();
    const store = useStore();
    const getUserInfo = async () => {
      try {
        const req = {
          uuid: store.state.userInfo.uuid,
        };
        const rsp = await axios.post(
          store.state.backendUrl + "/user/getUserInfo",
          req
        );
        if (rsp.data.code == 200) {
          if (!rsp.data.data.avatar.startsWith("http")) {
            rsp.data.data.avatar = store.state.backendUrl + rsp.data.data.avatar;
          }
          store.commit("setUserInfo", rsp.data.data);
        } else {
          console.error(rsp.data.message);
        }
      } catch (error) {
        console.log(error);
      }
    };
    const logout = async () => {
      store.commit("cleanUserInfo");
      const req = {
        owner_id: store.state.userInfo.uuid,
      };
      try {
        const rsp = await axios.post(
          store.state.backendUrl + "/user/wsLogout",
          req
        );
        if (rsp.data.code == 200) {
          router.push("/login");
          ElMessage.success("账号被封禁，退出登录");
        } else {
          ElMessage.error(rsp.data.message);
        }
      } catch (error) {
        console.log(error);
      }
    };
    onMounted(() => {
      if (store.state.userInfo.uuid) {
        getUserInfo();
        if (store.state.userInfo.status == 1) {
          logout();
        }
        const wsUrl =
          store.state.wsUrl + "/wss?token=" + store.state.userInfo.token;
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
      }
    });
  },
};
</script>

<style>
/* 全局基础样式由 assets/css/chat.css 提供 */
</style>