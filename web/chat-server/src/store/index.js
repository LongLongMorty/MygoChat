import { createStore } from 'vuex'

export default createStore({
  state: {
    // P0 修复：使用构建时环境变量，不再硬编码
    backendUrl: process.env.VUE_APP_API_BASE_URL || 'https://127.0.0.1:8000',
    wsUrl: process.env.VUE_APP_WS_URL || 'wss://127.0.0.1:8000',
    userInfo: (sessionStorage.getItem('userInfo') && JSON.parse(sessionStorage.getItem('userInfo'))) || {},
    socket: null,
  },
  getters: {
  },
  mutations: {
    setUserInfo(state, userInfo) {
      state.userInfo = userInfo;
      sessionStorage.setItem('userInfo', JSON.stringify(userInfo));
    },
    cleanUserInfo(state) {
      state.userInfo = {};
      sessionStorage.removeItem('userInfo');
    }
  },
  actions: {
  },
  modules: {
  }
})
